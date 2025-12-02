package orm

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/godev90/validator/faults"
	"github.com/lib/pq"
)

type (
	driverFlavor    int
	SqlQueryAdapter struct {
		db     *sql.DB
		ctx    context.Context
		flavor driverFlavor

		table      string
		fields     []string
		groups     []string
		havings    []string
		havingArgs []any
		joins      []string
		joinArgs   []any
		scopes     []ScopeFunc
		wheres     []string
		whereArgs  []any
		orWheres   []string
		orArgs     []any
		orderBy    string
		limit      *int
		offset     *int

		model Tabler
	}
)

const (
	FlavorMySQL driverFlavor = iota
	FlavorPostgres

	// Time format constants
	defaultTimeFormat = "2006-01-02 15:04:05"
	logSQLFormat      = "[sql] %s | %s\n"
	columnPrefix      = "column:"
)

var (
	errUnsupported = fmt.Errorf("orm: scan unsupported destination")
	ErrUnsupported = faults.New(errUnsupported, &faults.ErrAttr{
		Code: http.StatusInternalServerError,
	})

	errNilPointer = fmt.Errorf("orm: nil pointer")
	ErrNilPointer = faults.New(errNilPointer, &faults.ErrAttr{
		Code: http.StatusInternalServerError,
	})

	errTablerNotImplemented = fmt.Errorf("orm: tabler not implemented")
	ErrTablerNotImplemented = faults.New(errTablerNotImplemented, &faults.ErrAttr{
		Code: http.StatusInternalServerError,
	})

	errUnsupportedRaw = fmt.Errorf("orm: unsupported raw")
	ErrUnsupportedRaw = faults.New(errUnsupportedRaw, &faults.ErrAttr{
		Code: http.StatusInternalServerError,
		Messages: []faults.LangPackage{
			{
				Tag:     faults.English,
				Message: "orm: unsupported raw %T",
			},
		},
	})

	errParseTimeFailed = fmt.Errorf("orm: cannot parse time")
	ErrParseTimeFailed = faults.New(errParseTimeFailed, &faults.ErrAttr{
		Code: http.StatusInternalServerError,
		Messages: []faults.LangPackage{
			{
				Tag:     faults.English,
				Message: "orm: cannot parse time [%q]",
			},
		},
	})

	errUnsupportedKind = fmt.Errorf("orm: unsupported kind")
	ErrUnsupportedKind = faults.New(errUnsupportedKind, &faults.ErrAttr{
		Code: http.StatusInternalServerError,
		Messages: []faults.LangPackage{
			{
				Tag:     faults.English,
				Message: "orm: unsupported kind [%s]",
			},
		},
	})

	errNotFound = fmt.Errorf("orm: record not found")
	ErrNotFound = faults.New(errNotFound, &faults.ErrAttr{
		Code: http.StatusNotFound,
	})

	errParseFailed = fmt.Errorf("orm: parse failed")
	ErrParseFailed = faults.New(errParseFailed, &faults.ErrAttr{
		Code: http.StatusInternalServerError,
		Messages: []faults.LangPackage{
			{
				Tag:     faults.English,
				Message: "orm: cannot parse [%T] to [%s]",
			},
		},
	})
)

func detectFlavor(db *sql.DB) driverFlavor {
	t := strings.TrimPrefix(reflect.TypeOf(db.Driver()).String(), "*")
	switch {
	case strings.Contains(t, "pq"), strings.Contains(t, "pgx"), strings.Contains(t, "postgres"), strings.Contains(t, "stdlib"):
		return FlavorPostgres
	default:
		return FlavorMySQL
	}
}

// NewSqlAdapter wraps an existing *sql.DB.
func NewSqlAdapter(db *sql.DB) QueryAdapter {
	return &SqlQueryAdapter{
		db:       db,
		ctx:      context.Background(),
		flavor:   detectFlavor(db),
		fields:   []string{"*"},
		scopes:   []ScopeFunc{},
		joins:    []string{},
		joinArgs: []any{},
		wheres:   []string{},
		orWheres: []string{},
	}
}

func (q *SqlQueryAdapter) clone() *SqlQueryAdapter {
	cp := *q
	cp.fields = append([]string(nil), q.fields...)
	cp.joins = append([]string(nil), q.joins...)
	cp.joinArgs = append([]any(nil), q.joinArgs...)
	cp.wheres = append([]string(nil), q.wheres...)
	cp.whereArgs = append([]any(nil), q.whereArgs...)
	cp.orWheres = append([]string(nil), q.orWheres...)
	cp.orArgs = append([]any(nil), q.orArgs...)
	cp.scopes = append([]ScopeFunc(nil), q.scopes...)
	cp.model = q.model
	return &cp
}

func (q *SqlQueryAdapter) WithContext(ctx context.Context) QueryAdapter {
	cp := q.clone()
	cp.ctx = ctx
	return cp
}

func (q *SqlQueryAdapter) UseModel(m Tabler) QueryAdapter {
	cp := q.clone()
	cp.model = m
	cp.table = m.TableName()
	return cp
}

func (q *SqlQueryAdapter) Model() Tabler {
	return q.model
}

func (q *SqlQueryAdapter) Where(cond any, args ...any) QueryAdapter {
	cp := q.clone()

	// if sub, ok := cond.(*SqlQueryAdapter); ok {
	// 	var sb strings.Builder
	// 	sb.WriteString("(")

	// 	if len(sub.wheres) > 0 {
	// 		sb.WriteString(strings.Join(sub.wheres, " AND "))
	// 	}
	// 	if len(sub.orWheres) > 0 {
	// 		if len(sub.wheres) > 0 {
	// 			sb.WriteString(" OR ")
	// 		}
	// 		sb.WriteString("(")
	// 		sb.WriteString(strings.Join(sub.orWheres, " OR "))
	// 		sb.WriteString(")")
	// 	}
	// 	sb.WriteString(")")

	// 	cp.wheres = append(cp.wheres, sb.String())
	// 	cp.whereArgs = append(cp.whereArgs, sub.whereArgs...)
	// 	cp.whereArgs = append(cp.whereArgs, sub.orArgs...)
	// 	return cp
	// }

	if sub, ok := cond.(*SqlQueryAdapter); ok {
		// If sub was cloned from the same base, remove the common leading WHEREs
		subWheres := append([]string(nil), sub.wheres...)
		subWhereArgs := append([]any(nil), sub.whereArgs...)

		if sub.model == q.model && len(q.wheres) > 0 && len(sub.wheres) >= len(q.wheres) {
			common := 0
			argsToDrop := 0
			for i := 0; i < len(q.wheres) && i < len(sub.wheres); i++ {
				if sub.wheres[i] == q.wheres[i] {
					common++
					argsToDrop += strings.Count(q.wheres[i], "?")
				} else {
					break
				}
			}
			if common > 0 {
				subWheres = sub.wheres[common:]
				if argsToDrop <= len(sub.whereArgs) {
					subWhereArgs = sub.whereArgs[argsToDrop:]
				} else {
					subWhereArgs = nil
				}
			}
		}

		var sb strings.Builder
		sb.WriteString("(")

		if len(subWheres) > 0 {
			sb.WriteString(strings.Join(subWheres, " AND "))
		}
		if len(sub.orWheres) > 0 {
			if len(subWheres) > 0 {
				sb.WriteString(" OR ")
			}
			sb.WriteString("(")
			sb.WriteString(strings.Join(sub.orWheres, " OR "))
			sb.WriteString(")")
		}
		sb.WriteString(")")

		cp.wheres = append(cp.wheres, sb.String())
		cp.whereArgs = append(cp.whereArgs, subWhereArgs...)
		cp.whereArgs = append(cp.whereArgs, sub.orArgs...)
		return cp
	}

	condStr := toString(cond)
	finalArgs := make([]any, 0, len(args))

	for _, arg := range args {
		val := reflect.ValueOf(arg)
		if val.Kind() == reflect.Slice || val.Kind() == reflect.Array {
			// Handle slice/array
			if val.Len() == 0 {
				// Replace with something always false
				condStr = "1=0"
				continue
			}

			placeholders := make([]string, val.Len())
			for i := 0; i < val.Len(); i++ {
				placeholders[i] = "?"
				finalArgs = append(finalArgs, val.Index(i).Interface())
			}

			// Replace only the first "?" occurrence with expanded list
			condStr = strings.Replace(condStr, "?", "("+strings.Join(placeholders, ", ")+")", 1)
		} else {
			finalArgs = append(finalArgs, arg)
		}
	}

	cp.wheres = append(cp.wheres, condStr)
	cp.whereArgs = append(cp.whereArgs, finalArgs...)
	return cp
}

func (q *SqlQueryAdapter) Or(cond any, args ...any) QueryAdapter {
	cp := q.clone()
	cp.orWheres = append(cp.orWheres, toString(cond))
	cp.orArgs = append(cp.orArgs, args...)
	return cp
}

func (q *SqlQueryAdapter) Join(joinClause string, args ...any) QueryAdapter {
	// Automatically validate join clause for safety
	if err := ValidateJoinClause(joinClause); err != nil {
		// Return adapter unchanged if validation fails
		return q
	}
	cp := q.clone()
	cp.joins = append(cp.joins, joinClause)
	cp.joinArgs = append(cp.joinArgs, args...)
	return cp
}

func (q *SqlQueryAdapter) Select(sel []string) QueryAdapter {
	// Automatically sanitize select fields for safety
	sanitized, err := SanitizeSelectFields(sel)
	if err != nil {
		// Return adapter unchanged if sanitization fails
		return q
	}
	cp := q.clone()
	if len(sanitized) > 0 {
		cp.fields = sanitized
	}
	return cp
}

func (q *SqlQueryAdapter) GroupBy(cols []string) QueryAdapter {
	// Automatically sanitize group by fields for safety
	sanitized, err := SanitizeColumnNames(cols)
	if err != nil {
		// Return adapter unchanged if sanitization fails
		return q
	}
	cp := q.clone()
	if len(sanitized) > 0 {
		cp.groups = sanitized
	}
	return cp
}

func (q *SqlQueryAdapter) Having(cols []string, args ...any) QueryAdapter {
	// Automatically validate having clauses for safety
	if err := ValidateHavingClause(cols); err != nil {
		// Return adapter unchanged if validation fails
		return q
	}
	cp := q.clone()
	if len(cols) > 0 {
		cp.havings = cols
		cp.havingArgs = append(cp.havingArgs, args...)
	}
	return cp
}

func (q *SqlQueryAdapter) Limit(l int) QueryAdapter {
	cp := q.clone()
	cp.limit = &l
	return cp
}

func (q *SqlQueryAdapter) Offset(o int) QueryAdapter {
	cp := q.clone()
	cp.offset = &o
	return cp
}

func (q *SqlQueryAdapter) Order(order string) QueryAdapter {
	// Automatically validate order clause for safety
	if err := ValidateOrderBy(order); err != nil {
		// Return adapter unchanged if validation fails
		return q
	}
	cp := q.clone()
	cp.orderBy = order
	return cp
}

func (q *SqlQueryAdapter) Scopes(fs ...ScopeFunc) QueryAdapter {
	if len(fs) == 0 {
		return q
	}

	return func(q QueryAdapter, fs ...ScopeFunc) (out QueryAdapter) {
		out = q
		for _, f := range fs {
			if f == nil {
				continue
			}
			out = f(out)
		}

		return
	}(q, fs...)
}

func (q *SqlQueryAdapter) Clone() QueryAdapter {
	return q.UseModel(q.model)
}

func (q *SqlQueryAdapter) Count(target *int64) error {
	sqlStr, args := q.build(true)
	return q.db.QueryRowContext(q.ctx, sqlStr, args...).Scan(target)
}

func (g *SqlQueryAdapter) Driver() driverFlavor {
	return g.flavor
}

// Enhanced security methods implementation
func (q *SqlQueryAdapter) SafeOrder(order string) QueryAdapter {
	// Validate the order clause first
	if err := ValidateOrderBy(order); err != nil {
		// Return empty adapter or handle error appropriately
		// For now, we'll ignore invalid order clauses
		return q
	}
	return q.Order(order)
}

func (q *SqlQueryAdapter) SafeJoin(joinClause string, args ...any) QueryAdapter {
	// Validate the join clause first
	if err := ValidateJoinClause(joinClause); err != nil {
		// Return empty adapter or handle error appropriately
		return q
	}
	return q.Join(joinClause, args...)
}

func (q *SqlQueryAdapter) SafeSelect(selections []string) QueryAdapter {
	// Sanitize the select fields
	sanitized, err := SanitizeSelectFields(selections)
	if err != nil {
		// Return adapter with default fields on error
		return q
	}
	return q.Select(sanitized)
}

func (q *SqlQueryAdapter) SafeGroupBy(groupbys []string) QueryAdapter {
	// Sanitize the group by fields
	sanitized, err := SanitizeColumnNames(groupbys)
	if err != nil {
		// Return adapter unchanged on error
		return q
	}
	return q.GroupBy(sanitized)
}

func (q *SqlQueryAdapter) SafeHaving(havings []string, args ...any) QueryAdapter {
	// Validate the having clauses
	if err := ValidateHavingClause(havings); err != nil {
		// Return adapter unchanged on error
		return q
	}
	return q.Having(havings, args...)
}

// Unsafe methods for advanced users who want to bypass validation
func (q *SqlQueryAdapter) UnsafeOrder(order string) QueryAdapter {
	cp := q.clone()
	cp.orderBy = order
	return cp
}

func (q *SqlQueryAdapter) UnsafeJoin(joinClause string, args ...any) QueryAdapter {
	cp := q.clone()
	cp.joins = append(cp.joins, joinClause)
	cp.joinArgs = append(cp.joinArgs, args...)
	return cp
}

func (q *SqlQueryAdapter) UnsafeSelect(selections []string) QueryAdapter {
	cp := q.clone()
	if len(selections) > 0 {
		cp.fields = selections
	}
	return cp
}

func (q *SqlQueryAdapter) UnsafeGroupBy(groupbys []string) QueryAdapter {
	cp := q.clone()
	if len(groupbys) > 0 {
		cp.groups = groupbys
	}
	return cp
}

func (q *SqlQueryAdapter) UnsafeHaving(havings []string, args ...any) QueryAdapter {
	cp := q.clone()
	if len(havings) > 0 {
		cp.havings = havings
		cp.havingArgs = append(cp.havingArgs, args...)
	}
	return cp
}

func normalize(col string) string {
	col = strings.Trim(col, "`\"")
	if idx := strings.LastIndex(col, "."); idx != -1 {
		col = col[idx+1:]
	}
	return strings.ToLower(col)
}

func isEmptyRaw(v any) bool {
	switch b := v.(type) {
	case nil:
		return true
	case []byte:
		return len(b) == 0
	case sql.RawBytes:
		return len(b) == 0
	case string:
		return strings.TrimSpace(b) == ""
	default:
		return false
	}
}

var scannerT = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

func convertAssign(field reflect.Value, raw any) error {
	if raw == nil || isEmptyRaw(raw) {
		field.Set(reflect.Zero(field.Type()))
		return nil
	}

	if isScanner(field) {
		return assignWithScanner(field, raw)
	}

	if field.Kind() == reflect.Ptr {
		field.Set(reflect.New(field.Type().Elem()))
		return convertAssign(field.Elem(), raw)
	}

	switch field.Kind() {
	case reflect.String:
		return assignString(field, raw)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return assignInt(field, raw)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return assignUint(field, raw)
	case reflect.Float32, reflect.Float64:
		return assignFloat(field, raw)
	case reflect.Bool:
		return assignBool(field, raw)
	case reflect.Struct:
		if field.Type() == reflect.TypeOf(time.Time{}) {
			return assignTime(field, raw)
		}
		return assignJSON(field, raw)
	case reflect.Slice:
		return assignSlice(field, raw)
	default:
		return ErrUnsupportedKind.Render(field.Kind()) //fmt.Errorf("unsupported kind: %s", field.Kind())
	}
}

func isScanner(field reflect.Value) bool {
	if field.Kind() == reflect.Ptr && !field.IsNil() {
		return field.Type().Implements(scannerT)
	}
	if field.CanAddr() {
		return field.Addr().Type().Implements(scannerT)
	}
	return false
}

func assignWithScanner(field reflect.Value, raw any) error {
	val := toScalar(raw)
	if field.Kind() == reflect.Ptr {
		if field.IsNil() {
			field.Set(reflect.New(field.Type().Elem()))
		}
		return field.Interface().(sql.Scanner).Scan(val)
	}
	return field.Addr().Interface().(sql.Scanner).Scan(val)
}

func assignInt(field reflect.Value, raw any) error {
	scalar := toScalar(raw)

	switch v := scalar.(type) {
	case int64:
		field.SetInt(v)
	case float64:
		field.SetInt(int64(v))
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return err
		}
		field.SetInt(i)
	default:
		return ErrParseFailed.Render(scalar, "int") //fmt.Errorf("cannot assign %T to int", scalar)
	}
	return nil
}

func assignUint(field reflect.Value, raw any) error {
	scalar := toScalar(raw)

	switch v := scalar.(type) {
	case int64:
		field.SetUint(uint64(v))
	case float64:
		field.SetUint(uint64(v))
	case string:
		u, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
		field.SetUint(u)
	default:
		return ErrParseFailed.Render(scalar, "uint")
	}
	return nil
}

func assignFloat(field reflect.Value, raw any) error {
	scalar := toScalar(raw)

	switch v := scalar.(type) {
	case float64:
		field.SetFloat(v)
	case int64:
		field.SetFloat(float64(v))
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return err
		}
		field.SetFloat(f)
	default:
		return ErrParseFailed.Render(scalar, "float")
	}
	return nil
}

func assignBool(field reflect.Value, raw any) error {
	scalar := toScalar(raw)

	switch v := scalar.(type) {
	case bool:
		field.SetBool(v)
	case int64:
		field.SetBool(v != 0)
	case string:
		b, err := strconv.ParseBool(v)
		if err != nil {
			return err
		}
		field.SetBool(b)
	default:
		return ErrParseFailed.Render(scalar, "boolean")
	}
	return nil
}

func assignString(field reflect.Value, raw any) error {
	field.SetString(fmt.Sprint(toScalar(raw)))
	return nil
}

func assignSlice(field reflect.Value, raw any) error {
	switch v := raw.(type) {
	case sql.RawBytes:
		raw = []byte(v) // convert before scanning
	}

	switch field.Type().Elem().Kind() {
	case reflect.String:
		var result []string
		if err := pq.Array(&result).Scan(raw); err != nil {
			return err
		}
		field.Set(reflect.ValueOf(result))
		return nil

	case reflect.Int:
		var result []int64
		if err := pq.Array(&result).Scan(raw); err != nil {
			return err
		}
		slice := reflect.MakeSlice(field.Type(), len(result), len(result))
		for i, v := range result {
			slice.Index(i).SetInt(v)
		}
		field.Set(slice)
		return nil

	case reflect.Float64:
		var result []float64
		if err := pq.Array(&result).Scan(raw); err != nil {
			return err
		}
		field.Set(reflect.ValueOf(result))
		return nil

	default:
		return fmt.Errorf("unsupported slice element type: %s", field.Type().Elem().Kind())
	}
}

func assignJSON(field reflect.Value, raw any) error {
	rawStr := ""
	switch v := raw.(type) {
	case []byte:
		rawStr = string(v)
	case sql.RawBytes:
		rawStr = string(v)
	case string:
		rawStr = v
	default:
		return ErrParseFailed.Render(raw, "struct")
	}

	if strings.TrimSpace(rawStr) == "" {
		field.Set(reflect.Zero(field.Type()))
		return nil
	}

	ptr := reflect.New(field.Type()).Interface()
	if err := json.Unmarshal([]byte(rawStr), ptr); err != nil {
		return err
	}
	field.Set(reflect.ValueOf(ptr).Elem())
	return nil
}

func assignTime(field reflect.Value, raw any) error {
	scalar := toScalar(raw)

	switch v := scalar.(type) {
	case time.Time:
		field.Set(reflect.ValueOf(v))
		return nil
	case string:
		for _, layout := range []string{
			defaultTimeFormat,
			"2006-01-02T15:04:05Z",
			"2006-01-02",
			time.RFC3339,
		} {
			if t, err := time.ParseInLocation(layout, v, time.Local); err == nil {
				field.Set(reflect.ValueOf(t))
				return nil
			}
		}
		return fmt.Errorf("cannot parse time from string: %q", v)
	default:
		return ErrParseFailed.Render(scalar, "time")
	}
}

/* toScalar: aman untuk sql.RawBytes / []byte */
func toScalar(v any) any {
	switch b := v.(type) {
	case sql.RawBytes:
		return string([]byte(b))
	case []byte:
		return string(b)
	default:
		return v
	}
}

func (q *SqlQueryAdapter) Scan(dest any) error {
	// notFound := true

	if q.model == nil {
		if t, ok := dest.(Tabler); ok {
			q.model = t
			q.table = q.model.TableName()
		} else {
			return ErrTablerNotImplemented
		}
	}

	sqlStr, args := q.build(false)

	if debug {
		rendered := interpolate(sqlStr, args, q.flavor)
		start := time.Now()
		defer func() { log.Printf(logSQLFormat, rendered, time.Since(start)) }()
	}

	rows, err := q.db.QueryContext(q.ctx, sqlStr, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return ErrNilPointer
	}

	makeHolders := func() ([]any, []sql.RawBytes) {
		rawBytes := make([]sql.RawBytes, len(cols))
		holders := make([]any, len(cols))
		for i := range holders {
			holders[i] = &rawBytes[i]
		}
		return holders, rawBytes
	}

	switch val.Elem().Kind() {

	case reflect.Slice:
		slice := val.Elem()
		elemTyp := slice.Type().Elem()
		fieldMap := buildFieldMap(elemTyp)

		for rows.Next() {
			// notFound = false
			holders, raw := makeHolders()
			if err := rows.Scan(holders...); err != nil {
				return err
			}

			elemPtr := reflect.New(elemTyp)
			for ci, col := range cols {
				if fi, ok := fieldMap[normalize(col)]; ok {
					field := elemPtr.Elem().Field(fi)
					if err := convertAssign(field, raw[ci]); err != nil {
						return err
					}
				}
			}

			slice = reflect.Append(slice, elemPtr.Elem())
		}

		val.Elem().Set(slice)

		// if rows.Err() == nil && notFound {
		// 	return ErrNotFound
		// }

		return rows.Err()

	case reflect.Struct:
		if rows.Next() {
			// notFound = false
			holders, raw := makeHolders()
			if err := rows.Scan(holders...); err != nil {
				return err
			}

			fieldMap := buildFieldMap(val.Elem().Type())
			for ci, col := range cols {
				if fi, ok := fieldMap[normalize(col)]; ok {
					if err := convertAssign(val.Elem().Field(fi), raw[ci]); err != nil {
						return err
					}
				}
			}
		}

		// if rows.Err() == nil && notFound {
		// 	return ErrNotFound
		// }

		return rows.Err()
	}

	if mp, ok := dest.(*[]map[string]any); ok {
		for rows.Next() {
			// notFound = false
			holders, raw := makeHolders()
			if err := rows.Scan(holders...); err != nil {
				return err
			}

			rec := map[string]any{}
			for ci, col := range cols {
				if raw[ci] == nil {
					rec[col] = nil
				} else {
					rec[col] = string(raw[ci])
				}
			}
			*mp = append(*mp, rec)
		}

		// if rows.Err() == nil && notFound {
		// 	return ErrNotFound
		// }

		return rows.Err()
	}

	return ErrUnsupported
}

func (q *SqlQueryAdapter) First(dest any) error {
	if q.model == nil {
		if t, ok := dest.(Tabler); ok {
			q.model = t
			q.table = q.model.TableName()
		} else {
			return ErrTablerNotImplemented
		}
	}

	sqlStr, args := q.build(false)

	// Limit 1 jika belum ada
	if !strings.Contains(strings.ToLower(sqlStr), "limit") {
		sqlStr += " LIMIT 1"
	}

	if debug {
		rendered := interpolate(sqlStr, args, q.flavor)
		start := time.Now()
		defer func() { log.Printf(logSQLFormat, rendered, time.Since(start)) }()
	}

	rows, err := q.db.QueryContext(q.ctx, sqlStr, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		if rows.Err() != nil {
			return rows.Err()
		}
		return ErrNotFound
	}

	cols, _ := rows.Columns()
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return ErrNilPointer
	}

	holders := make([]any, len(cols))
	raw := make([]sql.RawBytes, len(cols))
	for i := range holders {
		holders[i] = &raw[i]
	}

	if err := rows.Scan(holders...); err != nil {
		return err
	}

	switch val.Elem().Kind() {
	case reflect.Struct:
		fieldMap := buildFieldMap(val.Elem().Type())
		for ci, col := range cols {
			if fi, ok := fieldMap[normalize(col)]; ok {
				if err := convertAssign(val.Elem().Field(fi), raw[ci]); err != nil {
					return err
				}
			}
		}
		return nil

	case reflect.Slice:
		// Ambil first element untuk slice
		elemTyp := val.Elem().Type().Elem()
		elemPtr := reflect.New(elemTyp)
		fieldMap := buildFieldMap(elemTyp)

		for ci, col := range cols {
			if fi, ok := fieldMap[normalize(col)]; ok {
				if err := convertAssign(elemPtr.Elem().Field(fi), raw[ci]); err != nil {
					return err
				}
			}
		}

		slice := reflect.MakeSlice(val.Elem().Type(), 1, 1)
		slice.Index(0).Set(elemPtr.Elem())
		val.Elem().Set(slice)
		return nil

	default:
		return ErrUnsupported
	}
}

type SqlTransactionAdapter struct {
	ctx    context.Context
	tx     *sql.Tx
	flavor driverFlavor
}

// func (q *SqlQueryAdapter) Begin() (*SqlTransactionAdapter, error) {
// 	tx, err := q.db.BeginTx(q.ctx, nil)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &SqlTransactionAdapter{
// 		ctx:    q.ctx,
// 		tx:     tx,
// 		flavor: q.flavor,
// 	}, nil
// }

func NewSqlTransactionAdapter(ctx context.Context, db *sql.DB) (*SqlTransactionAdapter, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &SqlTransactionAdapter{
		ctx:    ctx,
		tx:     tx,
		flavor: detectFlavor(db),
	}, nil
}

func (q *SqlTransactionAdapter) Tx() *sql.Tx {
	return q.tx
}

func (q *SqlTransactionAdapter) Commit() error {
	return q.tx.Commit()
}

func (q *SqlTransactionAdapter) Rollback() error {
	return q.tx.Rollback()
}

func (q *SqlTransactionAdapter) Create(src Tabler) error {
	val := reflect.ValueOf(src)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return ErrNilPointer
	}
	val = val.Elem()
	if val.Kind() != reflect.Struct {
		return ErrUnsupported
	}

	typ := val.Type()
	cols := []string{}
	placeholders := []string{}
	args := []any{}
	var pkFieldIndex int = -1
	var pkColumn string

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		if field.PkgPath != "" || field.Tag.Get("sql") == "-" {
			continue
		}

		col, _ := parseColumnTag(field)
		if col == "" {
			col = toSnake(field.Name)
		}

		fieldVal := val.Field(i)
		// Skip zero value on auto increment ID (e.g., primary key)
		if pk := strings.Contains(field.Tag.Get("sql"), "primaryKey"); pk {
			pkFieldIndex = i
			pkColumn = col
			continue
		}

		cols = append(cols, col)
		placeholders = append(placeholders, "?")
		args = append(args, fieldVal.Interface())
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		src.TableName(),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)

	if pkFieldIndex >= 0 && q.flavor == FlavorPostgres {
		query += fmt.Sprintf(" RETURNING %s", pkColumn)
	}

	if debug {
		start := time.Now()
		defer func() {
			log.Printf(logSQLFormat, logQueryWithValues(query, args), time.Since(start))
		}()
	}

	if q.flavor == FlavorPostgres {
		query = convertPostgresPlaceholder(query)
	}

	var err error
	if pkFieldIndex >= 0 && q.flavor == FlavorPostgres {
		err = q.tx.QueryRowContext(q.ctx, query, args...).Scan(val.Field(pkFieldIndex).Addr().Interface())
	} else {
		result, execErr := q.tx.ExecContext(q.ctx, query, args...)
		err = execErr
		if execErr == nil && pkFieldIndex >= 0 {
			if lastID, idErr := result.LastInsertId(); idErr == nil {
				val.Field(pkFieldIndex).SetInt(lastID)
			}
		}
	}

	return err
}

func (q *SqlTransactionAdapter) Patch(src Tabler, fields map[string]any) error {
	val := reflect.ValueOf(src)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return ErrNilPointer
	}
	val = val.Elem()
	if val.Kind() != reflect.Struct {
		return ErrUnsupported
	}

	typ := val.Type()

	var pkCol string
	var pkVal any
	validCols := map[string]struct{}{}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" || field.Tag.Get("sql") == "-" {
			continue
		}

		col, isPK := parseColumnTag(field)
		if col == "" {
			col = toSnake(field.Name)
		}

		if isPK {
			pkCol = col
			pkVal = val.Field(i).Interface()
		}

		validCols[col] = struct{}{}
	}

	if pkCol == "" {
		return faults.New(fmt.Errorf("orm: primary key not found"), &faults.ErrAttr{
			Code: http.StatusBadRequest,
		})
	}

	cols := []string{}
	args := []any{}

	for col, v := range fields {
		if _, ok := validCols[col]; !ok {
			return faults.New(fmt.Errorf("invalid column: %s", col), &faults.ErrAttr{
				Code: http.StatusBadRequest,
			})
		}
		cols = append(cols, fmt.Sprintf("%s = ?", col))
		args = append(args, v)
	}
	args = append(args, pkVal)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?",
		src.TableName(),
		strings.Join(cols, ", "),
		pkCol,
	)

	if debug {
		start := time.Now()
		defer func() {
			log.Printf(logSQLFormat, logQueryWithValues(query, args), time.Since(start))
		}()
	}

	if q.flavor == FlavorPostgres {
		query = convertPostgresPlaceholder(query)
	}

	_, err := q.tx.ExecContext(q.ctx, query, args...)
	return err
}

func (q *SqlTransactionAdapter) Update(src Tabler) error {
	val := reflect.ValueOf(src)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return ErrNilPointer
	}
	val = val.Elem()
	if val.Kind() != reflect.Struct {
		return ErrUnsupported
	}

	typ := val.Type()

	var pkCol string
	var pkVal any
	cols := []string{}
	args := []any{}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" || field.Tag.Get("sql") == "-" {
			continue
		}

		col, isPK := parseColumnTag(field)
		if col == "" {
			col = toSnake(field.Name)
		}

		value := val.Field(i).Interface()

		if isPK {
			pkCol = col
			pkVal = value
			continue // primary key tidak ikut di SET
		}

		cols = append(cols, fmt.Sprintf("%s = ?", col))
		args = append(args, value)
	}

	if pkCol == "" {
		return faults.New(fmt.Errorf("orm: primary key not found"), &faults.ErrAttr{
			Code: http.StatusBadRequest,
		})
	}

	args = append(args, pkVal)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?",
		src.TableName(),
		strings.Join(cols, ", "),
		pkCol,
	)

	if debug {
		start := time.Now()
		defer func() {
			log.Printf(logSQLFormat, logQueryWithValues(query, args), time.Since(start))
		}()
	}

	if q.flavor == FlavorPostgres {
		query = convertPostgresPlaceholder(query)
	}

	_, err := q.tx.ExecContext(q.ctx, query, args...)
	return err
}

func (q *SqlTransactionAdapter) BulkInsert(models []Tabler) error {
	if len(models) == 0 {
		return nil
	}

	first := models[0]
	val := reflect.ValueOf(first)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return ErrNilPointer
	}
	val = val.Elem()
	if val.Kind() != reflect.Struct {
		return ErrUnsupported
	}

	typ := val.Type()
	cols := []string{}
	fieldIndexes := []int{}

	// Determine columns and indexes once from first struct
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		if field.PkgPath != "" || field.Tag.Get("sql") == "-" {
			continue
		}

		if strings.Contains(field.Tag.Get("sql"), "primaryKey") {
			continue
		}

		col, _ := parseColumnTag(field)
		if col == "" {
			col = toSnake(field.Name)
		}
		cols = append(cols, col)
		fieldIndexes = append(fieldIndexes, i)
	}

	if len(cols) == 0 {
		return fmt.Errorf("orm: no insertable fields found")
	}

	table := first.TableName()
	// if table == "" {
	// 	if tabler, ok := first.(Tabler); ok {

	// 	} else {
	// 		return ErrTablerNotImplemented
	// 	}
	// }

	placeholderRows := []string{}
	args := []any{}

	for _, model := range models {
		v := reflect.ValueOf(model)
		if v.Kind() != reflect.Ptr || v.IsNil() {
			return ErrNilPointer
		}
		v = v.Elem()
		if v.Kind() != reflect.Struct {
			return ErrUnsupported
		}

		ph := []string{}
		for _, idx := range fieldIndexes {
			fieldVal := v.Field(idx)
			ph = append(ph, "?")
			args = append(args, fieldVal.Interface())
		}
		placeholderRows = append(placeholderRows, fmt.Sprintf("(%s)", strings.Join(ph, ", ")))
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		table,
		strings.Join(cols, ", "),
		strings.Join(placeholderRows, ", "),
	)

	if debug {
		start := time.Now()
		defer func() {
			log.Printf(logSQLFormat, logQueryWithValues(query, args), time.Since(start))
		}()
	}

	if q.flavor == FlavorPostgres {
		query = convertPostgresPlaceholder(query)
	}

	_, err := q.tx.ExecContext(q.ctx, query, args...)
	return err
}

func logQueryWithValues(query string, args []any) string {
	var sb strings.Builder
	argIdx := 0

	for i := 0; i < len(query); i++ {
		if query[i] == '?' && argIdx < len(args) {
			sb.WriteString(formatSQLValue(args[argIdx]))
			argIdx++
		} else {
			sb.WriteByte(query[i])
		}
	}
	return sb.String()
}

func formatSQLValue(v any) string {
	switch val := v.(type) {
	case nil:
		return "NULL"
	case *int, *int64, *int32:
		if reflect.ValueOf(val).IsNil() {
			return "NULL"
		}
		return fmt.Sprintf("%v", reflect.ValueOf(val).Elem())
	case *string:
		if val == nil {
			return "NULL"
		}
		return "'" + strings.ReplaceAll(*val, "'", "''") + "'"
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case time.Time:
		return "'" + val.Format(defaultTimeFormat) + "'"
	case fmt.Stringer:
		return "'" + strings.ReplaceAll(val.String(), "'", "''") + "'"
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr {
			if rv.IsNil() {
				return "NULL"
			}
			return formatSQLValue(rv.Elem().Interface())
		}
		return fmt.Sprintf("%v", v)
	}
}

func convertPostgresPlaceholder(query string) string {
	var result strings.Builder
	argIndex := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			result.WriteString(fmt.Sprintf("$%d", argIndex))
			argIndex++
		} else {
			result.WriteByte(query[i])
		}
	}
	return result.String()
}

func interpolate(sqlStr string, args []any, flavor driverFlavor) string {
	var out strings.Builder
	argIdx := 0

	quote := func(a any) string {
		switch v := a.(type) {
		case string:
			return "'" + strings.ReplaceAll(v, "'", "''") + "'" // escape '
		case time.Time:
			return "'" + v.Format(defaultTimeFormat) + "'"
		default:
			return fmt.Sprint(v)
		}
	}

	switch flavor {

	case FlavorPostgres:
		re := regexp.MustCompile(`\$\d+`)
		out.WriteString(re.ReplaceAllStringFunc(sqlStr, func(_ string) string {
			if argIdx >= len(args) {
				return "?"
			}
			val := quote(args[argIdx])
			argIdx++
			return val
		}))
		return out.String()

	default:
		for i := 0; i < len(sqlStr); i++ {
			if sqlStr[i] == '?' && argIdx < len(args) {
				out.WriteString(quote(args[argIdx]))
				argIdx++
			} else {
				out.WriteByte(sqlStr[i])
			}
		}
		return out.String()
	}
}

func (q *SqlQueryAdapter) build(count bool) (string, []any) {
	var sb strings.Builder
	if count {
		sb.WriteString("SELECT COUNT(1) FROM ")
	} else {
		sb.WriteString("SELECT ")
		sb.WriteString(strings.Join(q.fields, ", "))
		sb.WriteString(" FROM ")
	}
	sb.WriteString(q.table)

	if len(q.joins) > 0 {
		sb.WriteByte(' ')
		sb.WriteString(strings.Join(q.joins, " "))
	}

	args := make([]any, 0, len(q.joinArgs)+len(q.whereArgs)+len(q.orArgs))
	args = append(args, q.joinArgs...)

	if len(q.wheres) > 0 || len(q.orWheres) > 0 {
		sb.WriteString(" WHERE ")
		if len(q.wheres) > 0 {
			sb.WriteString(strings.Join(q.wheres, " AND "))
			args = append(args, q.whereArgs...)
		}
		if len(q.orWheres) > 0 {
			if len(q.wheres) > 0 {
				sb.WriteString(" OR ")
			}
			sb.WriteString("(")
			sb.WriteString(strings.Join(q.orWheres, " OR "))
			sb.WriteString(")")
			args = append(args, q.orArgs...)
		}
	}

	if len(q.groups) > 0 && !count {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(strings.Join(q.groups, ", "))
	}

	if len(q.havings) > 0 && !count {
		sb.WriteString(" HAVING ")
		sb.WriteString(strings.Join(q.havings, ", "))
		args = append(args, q.havingArgs...)
	}

	if q.orderBy != "" && !count {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(q.orderBy)
	}
	if q.limit != nil && !count {
		sb.WriteString(" LIMIT ")
		sb.WriteString(fmt.Sprint(*q.limit))
	}
	if q.offset != nil && !count {
		sb.WriteString(" OFFSET ")
		sb.WriteString(fmt.Sprint(*q.offset))
	}

	sqlStr := sb.String()
	if q.flavor == FlavorPostgres {
		// replace ? with $n
		var idx int
		var b strings.Builder
		for i := 0; i < len(sqlStr); i++ {
			if sqlStr[i] == '?' {
				idx++
				b.WriteString("$")
				b.WriteString(fmt.Sprint(idx))
			} else {
				b.WriteByte(sqlStr[i])
			}
		}
		sqlStr = b.String()
	}
	return sqlStr, args
}

func buildFieldMap(t reflect.Type) map[string]int {
	m := map[string]int{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}

		if f.Tag.Get("sql") == "-" {
			continue
		}

		col, _ := parseColumnTag(f)
		if col == "" {
			col = toSnake(f.Name)
		}
		m[strings.ToLower(col)] = i
	}
	return m
}

func parseColumnTag(f reflect.StructField) (string, bool) {
	extract := func(tag string) (string, bool) {
		if strings.Contains(tag, columnPrefix) {
			for _, p := range strings.Split(tag, ";") {
				if strings.HasPrefix(p, columnPrefix) {
					return strings.TrimPrefix(p, columnPrefix), strings.Contains(tag, "primaryKey")
				}
			}
		} else if !strings.Contains(tag, ":") {
			return tag, false
		}
		return "", false
	}

	if tag := f.Tag.Get("sql"); tag != "" {
		if col, pk := extract(tag); col != "" {
			return col, pk
		}
	}

	return "", false
}

func toSnake(s string) string {
	var out []rune
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			out = append(out, '_')
		}
		out = append(out, r)
	}
	return strings.ToLower(string(out))
}

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		return fmt.Sprint(t)
	}
}

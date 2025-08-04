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

		table     string
		fields    []string
		joins     []string
		joinArgs  []any
		scopes    []ScopeFunc
		wheres    []string
		whereArgs []any
		orWheres  []string
		orArgs    []any
		orderBy   string
		limit     *int
		offset    *int

		model Tabler
	}
)

const (
	flavorMySQL driverFlavor = iota
	flavorPostgres
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
				Message: "orm: cannot parse time %q",
			},
		},
	})

	errUnsupportedKind = fmt.Errorf("orm: unsupported kind")
	ErrUnsupportedKind = faults.New(errUnsupportedKind, &faults.ErrAttr{
		Code: http.StatusInternalServerError,
		Messages: []faults.LangPackage{
			{
				Tag:     faults.English,
				Message: "orm: unsupported kind %s",
			},
		},
	})

	errNotFound = fmt.Errorf("orm: record not found")
	ErrNotFound = faults.New(errNotFound, &faults.ErrAttr{
		Code: http.StatusNotFound,
	})
)

func detectFlavor(db *sql.DB) driverFlavor {
	t := strings.TrimPrefix(reflect.TypeOf(db.Driver()).String(), "*")
	switch {
	case strings.Contains(t, "pq"), strings.Contains(t, "pgx"), strings.Contains(t, "postgres"), strings.Contains(t, "stdlib"):
		return flavorPostgres
	default:
		return flavorMySQL
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

	if sub, ok := cond.(*SqlQueryAdapter); ok {
		var sb strings.Builder
		sb.WriteString("(")

		if len(sub.wheres) > 0 {
			sb.WriteString(strings.Join(sub.wheres, " AND "))
		}
		if len(sub.orWheres) > 0 {
			if len(sub.wheres) > 0 {
				sb.WriteString(" OR ")
			}
			sb.WriteString("(")
			sb.WriteString(strings.Join(sub.orWheres, " OR "))
			sb.WriteString(")")
		}
		sb.WriteString(")")

		cp.wheres = append(cp.wheres, sb.String())
		cp.whereArgs = append(cp.whereArgs, sub.whereArgs...)
		cp.whereArgs = append(cp.whereArgs, sub.orArgs...)
		return cp
	}

	cp.wheres = append(cp.wheres, toString(cond))
	cp.whereArgs = append(cp.whereArgs, args...)
	return cp
}

func (q *SqlQueryAdapter) Or(cond any, args ...any) QueryAdapter {
	cp := q.clone()
	cp.orWheres = append(cp.orWheres, toString(cond))
	cp.orArgs = append(cp.orArgs, args...)
	return cp
}

func (q *SqlQueryAdapter) Join(joinClause string, args ...any) QueryAdapter {
	cp := q.clone()
	cp.joins = append(cp.joins, joinClause)
	cp.joinArgs = append(cp.joinArgs, args...)
	return cp
}

func (q *SqlQueryAdapter) Select(sel []string) QueryAdapter {
	cp := q.clone()
	if len(sel) > 0 {
		cp.fields = sel
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
	cp := q.clone()
	cp.orderBy = order
	return cp
}

func (q *SqlQueryAdapter) Scopes(fs ...ScopeFunc) QueryAdapter {
	if len(fs) == 0 {
		return q
	}

	tmp := q.clone()
	tmp.wheres, tmp.whereArgs = nil, nil
	tmp.orWheres, tmp.orArgs = nil, nil

	tmp = applyScopes(tmp, fs...).(*SqlQueryAdapter)

	return q.Where(tmp)
}

func (q *SqlQueryAdapter) Clone() QueryAdapter {
	return q.clone()
}

func (q *SqlQueryAdapter) Count(target *int64) error {
	sqlStr, args := q.build(true)
	return q.db.QueryRowContext(q.ctx, sqlStr, args...).Scan(target)
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

// func convertAssign(field reflect.Value, raw any) error {
// 	if raw == nil {
// 		field.Set(reflect.Zero(field.Type()))
// 		return nil
// 	}

// 	isPtr := field.Kind() == reflect.Ptr

// 	if isPtr && field.Type().Implements(scannerT) {
// 		if isEmptyRaw(raw) {
// 			field.Set(reflect.Zero(field.Type()))
// 			return nil
// 		}
// 		if field.IsNil() {
// 			field.Set(reflect.New(field.Type().Elem()))
// 		}
// 		return field.Interface().(sql.Scanner).Scan(toScalar(raw))
// 	}

// 	if field.CanAddr() && field.Addr().Type().Implements(scannerT) {
// 		if isEmptyRaw(raw) {
// 			// value non-pointer di-zero-kan
// 			field.Set(reflect.Zero(field.Type()))
// 			return nil
// 		}
// 		return field.Addr().Interface().(sql.Scanner).Scan(toScalar(raw))
// 	}

// 	if isPtr {
// 		if isEmptyRaw(raw) {
// 			field.Set(reflect.Zero(field.Type()))
// 			return nil
// 		}
// 		field.Set(reflect.New(field.Type().Elem()))
// 		return convertAssign(field.Elem(), raw)
// 	}

// 	if tm, ok := raw.(time.Time); ok && field.Type() == reflect.TypeOf(time.Time{}) {
// 		field.Set(reflect.ValueOf(tm))
// 		return nil
// 	}

// 	var str string
// 	switch v := raw.(type) {
// 	case []byte:
// 		str = string(v)
// 	case sql.RawBytes:
// 		str = string(v)
// 	case string:
// 		str = v
// 	default:
// 		return ErrUnsupportedRaw.Render(raw)
// 	}
// 	if strings.TrimSpace(str) == "" {
// 		field.Set(reflect.Zero(field.Type()))
// 		return nil
// 	}

// 	// 6. set sesuai kind (string/int/uint/float/bool/time)
// 	switch field.Kind() {
// 	case reflect.String:
// 		field.SetString(str)

// 	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
// 		i, err := strconv.ParseInt(str, 10, 64)
// 		if err != nil {
// 			return err
// 		}
// 		field.SetInt(i)

// 	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
// 		u, err := strconv.ParseUint(str, 10, 64)
// 		if err != nil {
// 			return err
// 		}
// 		field.SetUint(u)

// 	case reflect.Float32, reflect.Float64:
// 		f, err := strconv.ParseFloat(str, 64)
// 		if err != nil {
// 			return err
// 		}
// 		field.SetFloat(f)

// 	case reflect.Bool:
// 		field.SetBool(str == "1" || strings.EqualFold(str, "true"))

// 	case reflect.Struct:
// 		if field.Type() == reflect.TypeOf(time.Time{}) {
// 			for _, layout := range []string{
// 				"2006-01-02 15:04:05",
// 				"2006-01-02T15:04:05Z",
// 				"2006-01-02",
// 				time.RFC3339,
// 			} {
// 				if t, err := time.ParseInLocation(layout, str, time.Local); err == nil {
// 					field.Set(reflect.ValueOf(t))
// 					return nil
// 				}
// 			}
// 			return ErrParseTimeFailed.Render(str)
// 		}
// 		fallthrough

// 	default:
// 		return ErrUnsupportedKind.Render(field.Kind().String())
// 	}

// 	return nil
// }

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
		return fmt.Errorf("unsupported kind: %s", field.Kind())
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
		return fmt.Errorf("cannot assign %T to int", scalar)
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
		return fmt.Errorf("cannot assign %T to uint", scalar)
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
		return fmt.Errorf("cannot assign %T to float", scalar)
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
		return fmt.Errorf("cannot assign %T to bool", scalar)
	}
	return nil
}

func assignString(field reflect.Value, raw any) error {
	field.SetString(fmt.Sprint(toScalar(raw)))
	return nil
}

func assignSlice(field reflect.Value, raw any) error {
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
		return fmt.Errorf("cannot assign %T to struct", raw)
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
			"2006-01-02 15:04:05",
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
		return fmt.Errorf("cannot assign %T to time.Time", scalar)
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
		defer func() { log.Printf("[sql] %s | %s\n", rendered, time.Since(start)) }()
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
		defer func() { log.Printf("[sql] %s | %s\n", rendered, time.Since(start)) }()
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

	if pkFieldIndex >= 0 && q.flavor == flavorPostgres {
		query += fmt.Sprintf(" RETURNING %s", pkColumn)
	}

	if debug {
		start := time.Now()
		defer func() {
			log.Printf("[sql] %s | %s\n", logQueryWithValues(query, args), time.Since(start))
		}()
	}

	if q.flavor == flavorPostgres {
		query = convertPostgresPlaceholder(query)
	}

	var err error
	if pkFieldIndex >= 0 && q.flavor == flavorPostgres {
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
			log.Printf("[sql] %s | %s\n", logQueryWithValues(query, args), time.Since(start))
		}()
	}

	if q.flavor == flavorPostgres {
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
			log.Printf("[sql] %s | %s\n", logQueryWithValues(query, args), time.Since(start))
		}()
	}

	if q.flavor == flavorPostgres {
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
			log.Printf("[sql] %s | %s\n", logQueryWithValues(query, args), time.Since(start))
		}()
	}

	if q.flavor == flavorPostgres {
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
		return "'" + val.Format("2006-01-02 15:04:05") + "'"
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
			return "'" + v.Format("2006-01-02 15:04:05") + "'"
		default:
			return fmt.Sprint(v)
		}
	}

	switch flavor {

	case flavorPostgres:
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
	if q.flavor == flavorPostgres {
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
		if strings.Contains(tag, "column:") {
			for _, p := range strings.Split(tag, ";") {
				if strings.HasPrefix(p, "column:") {
					return strings.TrimPrefix(p, "column:"), strings.Contains(tag, "primaryKey")
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

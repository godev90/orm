package orm

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/godev90/validator/errors"
)

type driverFlavor int

const (
	flavorMySQL driverFlavor = iota
	flavorPostgres
)

func detectFlavor(db *sql.DB) driverFlavor {
	t := strings.TrimPrefix(reflect.TypeOf(db.Driver()).String(), "*")
	switch {
	case strings.Contains(t, "pq"), strings.Contains(t, "pgx"), strings.Contains(t, "postgres"):
		return flavorPostgres
	default:
		return flavorMySQL
	}
}

type SqlQueryAdapter struct {
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

// NewSqlAdapter wraps an existing *sql.DB.
func NewSqlAdapter(db *sql.DB) *SqlQueryAdapter {
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
	cp.table = m.TableName()
	return cp
}

func (q *SqlQueryAdapter) Model() Tabler {
	return q.model
}

func (q *SqlQueryAdapter) Where(cond any, args ...any) QueryAdapter {
	cp := q.clone()

	if sub, ok := cond.(*SqlQueryAdapter); ok {
		// Bangun string kondisi sub-query
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

func (q *SqlQueryAdapter) Clone() QueryAdapter { return q.clone() }

func (q *SqlQueryAdapter) Count(target *int64) error {
	sqlStr, args := q.build(true)
	return q.db.QueryRowContext(q.ctx, sqlStr, args...).Scan(target)
}

func (q *SqlQueryAdapter) Scan(dest any) error {
	sqlStr, args := q.build(false)

	if debug {
		rendered := interpolate(sqlStr, args, q.flavor)
		start := time.Now()
		defer func() {
			log.Printf("[sql] %s | %s\n", rendered, time.Since(start))
		}()
	}

	rows, err := q.db.QueryContext(q.ctx, sqlStr, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return fmt.Errorf("adapter: dest must be non-nil pointer")
	}

	switch val.Elem().Kind() {
	case reflect.Slice:
		slice := val.Elem()
		elemTyp := slice.Type().Elem()
		for rows.Next() {
			elemPtr := reflect.New(elemTyp)
			targets, e := makeScanTargets(elemPtr.Interface(), cols)
			if e != nil {
				return e
			}
			if err := rows.Scan(targets...); err != nil {
				return err
			}
			slice = reflect.Append(slice, elemPtr.Elem())
		}
		val.Elem().Set(slice)
		return rows.Err()

	case reflect.Struct:
		if rows.Next() {
			targets, e := makeScanTargets(dest, cols)
			if e != nil {
				return e
			}
			if err := rows.Scan(targets...); err != nil {
				return err
			}
		}
		return rows.Err()
	}

	if mp, ok := dest.(*[]map[string]any); ok {
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return err
			}
			rec := map[string]any{}
			for i, c := range cols {
				rec[c] = vals[i]
			}
			*mp = append(*mp, rec)
		}
		return rows.Err()
	}

	return errUnsupported
}

var errUnsupported = errors.New(fmt.Errorf("adapter: Scan unsupported destination"), &errors.ErrAttr{
	Code: http.StatusInternalServerError,
})

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

func makeScanTargets(dest any, cols []string) ([]any, error) {
	val := reflect.ValueOf(dest)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil, errUnsupported
	}
	fieldMap := buildFieldMap(val.Type())
	targets := make([]any, len(cols))
	for i, c := range cols {
		if idx, ok := fieldMap[strings.ToLower(c)]; ok {
			targets[i] = val.Field(idx).Addr().Interface()
		} else {
			var dummy any
			targets[i] = &dummy
		}
	}
	return targets, nil
}

func buildFieldMap(t reflect.Type) map[string]int {
	m := map[string]int{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		if f.Tag.Get("gorm") == "-" || f.Tag.Get("sql") == "-" || f.Tag.Get("column") == "-" {
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
					return strings.TrimPrefix(p, "column:"), strings.Contains(tag, "primary")
				}
			}
		} else if !strings.Contains(tag, ":") {
			return tag, false
		}
		return "", false
	}
	if tag := f.Tag.Get("gorm"); tag != "" {
		if col, pk := extract(tag); col != "" {
			return col, pk
		}
	}
	if tag := f.Tag.Get("sql"); tag != "" {
		if col, pk := extract(tag); col != "" {
			return col, pk
		}
	}
	if tag := f.Tag.Get("column"); tag != "" {
		return tag, false
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

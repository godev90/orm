package orm

import (
	"context"
	"reflect"
	"regexp"
	"strings"
)

type (
	Tabler interface {
		TableName() string
	}

	QueryAdapter interface {
		WithContext(ctx context.Context) QueryAdapter
		Count(target *int64) error
		Limit(limit int) QueryAdapter
		Offset(offset int) QueryAdapter
		Order(order string) QueryAdapter
		Scan(dest any) error
		First(dest any) error
		Model() Tabler
		UseModel(Tabler) QueryAdapter
		Join(joinClause string, args ...any) QueryAdapter
		Scopes(fs ...ScopeFunc) QueryAdapter
		Where(query any, args ...any) QueryAdapter
		Or(query any, args ...any) QueryAdapter
		Select(selections []string) QueryAdapter
		Clone() QueryAdapter
		Driver() driverFlavor
	}

	ScopeFunc func(QueryAdapter) QueryAdapter
)

func applyScopes(a QueryAdapter, fs ...ScopeFunc) QueryAdapter {
	for _, f := range fs {
		a = f(a)
	}
	return a
}

func DefaultGormTablerAllowedFields(model Tabler) map[string]string {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	fields := make(map[string]string)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		jsonName := f.Tag.Get("json")
		if jsonName == "" || jsonName == "-" {
			jsonName = strings.ToLower(f.Name)
		}
		gormTag := f.Tag.Get("gorm")
		var columnName string
		if gormTag != "" {
			for _, part := range strings.Split(gormTag, ";") {
				if strings.HasPrefix(part, "column:") {
					columnName = strings.TrimPrefix(part, "column:")
					break
				}
			}
		}
		if columnName != "" && regexp.MustCompile(`^[a-zA-Z0-9_]+$`).MatchString(columnName) {
			fields[jsonName] = columnName
		}
	}
	return fields
}

func DefaultSqlTablerAllowedFields(model Tabler) map[string]string {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	fields := make(map[string]string)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		jsonName := f.Tag.Get("json")
		if jsonName == "" || jsonName == "-" {
			jsonName = strings.ToLower(f.Name)
		}
		sqlTag := f.Tag.Get("sql")
		var columnName string
		if sqlTag != "" {
			for _, part := range strings.Split(sqlTag, ";") {
				if strings.HasPrefix(part, "column:") {
					columnName = strings.TrimPrefix(part, "column:")
					break
				}
			}
		}
		if columnName != "" && regexp.MustCompile(`^[a-zA-Z0-9_]+$`).MatchString(columnName) {
			fields[jsonName] = columnName
		}
	}
	return fields
}

var debug = false

func DebugOn() {
	debug = true
}

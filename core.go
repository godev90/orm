package orm

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

// Constants for security validation
const (
	columnTagPrefix   = "column:"
	maxIdentifierLen  = 64
	maxOrderByLen     = 256
	maxJoinClauseLen  = 512
	maxWhereClauseLen = 1024
)

// Security validation patterns
var (
	// SQL identifier pattern (table names, column names)
	sqlIdentifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

	// Order by pattern (column ASC/DESC, with optional table prefix)
	orderByPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?\s*(ASC|DESC)?(\s*,\s*[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?\s*(ASC|DESC)?)*$`)

	// Basic JOIN pattern validation
	joinPattern = regexp.MustCompile(`^(INNER|LEFT|RIGHT|FULL\s+OUTER)?\s*JOIN\s+[a-zA-Z_][a-zA-Z0-9_]*(\s+AS\s+[a-zA-Z_][a-zA-Z0-9_]*)?\s+ON\s+.+$`)

	// Column name validation
	columnNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
)

// Security errors
var (
	ErrInvalidIdentifier = errors.New("orm: invalid SQL identifier")
	ErrInvalidOrderBy    = errors.New("orm: invalid ORDER BY clause")
	ErrInvalidJoinClause = errors.New("orm: invalid JOIN clause")
	ErrInvalidColumnName = errors.New("orm: invalid column name")
	ErrIdentifierTooLong = errors.New("orm: identifier too long")
	ErrSuspiciousPattern = errors.New("orm: suspicious SQL pattern detected")
)

// Common suspicious patterns for validation
var (
	commonSuspiciousPatterns = []string{
		"--", "/*", "*/", ";", "UNION", "DELETE", "DROP", "INSERT", "UPDATE",
		"EXEC", "EXECUTE", "DECLARE", "SELECT", "CREATE", "ALTER", "TRUNCATE",
	}

	databaseMetadataPatterns = []string{
		"INFORMATION_SCHEMA", "SYS.", "MYSQL.", "PG_",
	}
)

// Helper functions for validation
func validateSuspiciousPatterns(input string, patterns []string) error {
	upperInput := strings.ToUpper(input)
	for _, pattern := range patterns {
		if strings.Contains(upperInput, pattern) {
			return ErrSuspiciousPattern
		}
	}
	return nil
}

func validateLength(input string, maxLen int, errType error) error {
	if len(input) > maxLen {
		return errType
	}
	return nil
}

type (
	Tabler interface {
		TableName() string
	}

	QueryAdapter interface {
		WithContext(ctx context.Context) QueryAdapter
		Count(target *int64) error
		Limit(limit int) QueryAdapter
		Offset(offset int) QueryAdapter
		// Core methods are now automatically safe
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
		GroupBy(groupbys []string) QueryAdapter
		Having(havings []string, args ...any) QueryAdapter
		Clone() QueryAdapter
		Driver() driverFlavor
		DB() *sql.DB

		// Safe methods for backward compatibility and explicit safety
		SafeOrder(order string) QueryAdapter
		SafeJoin(joinClause string, args ...any) QueryAdapter
		SafeSelect(selections []string) QueryAdapter
		SafeGroupBy(groupbys []string) QueryAdapter
		SafeHaving(havings []string, args ...any) QueryAdapter

		// Unsafe methods for advanced users who want to bypass validation
		UnsafeOrder(order string) QueryAdapter
		UnsafeJoin(joinClause string, args ...any) QueryAdapter
		UnsafeSelect(selections []string) QueryAdapter
		UnsafeGroupBy(groupbys []string) QueryAdapter
		UnsafeHaving(havings []string, args ...any) QueryAdapter
	}

	ScopeFunc func(QueryAdapter) QueryAdapter
)

// Security validation functions
func ValidateOrderBy(orderBy string) error {
	if len(orderBy) == 0 {
		return nil
	}

	if err := validateLength(orderBy, maxOrderByLen, ErrInvalidOrderBy); err != nil {
		return err
	}

	if err := validateSuspiciousPatterns(orderBy, commonSuspiciousPatterns); err != nil {
		return err
	}

	return validateOrderByFormat(orderBy)
}

func validateOrderByFormat(orderBy string) error {
	if !orderByPattern.MatchString(strings.TrimSpace(orderBy)) {
		return ErrInvalidOrderBy
	}
	return nil
}

func ValidateJoinClause(joinClause string) error {
	if len(joinClause) == 0 {
		return nil
	}

	if err := validateLength(joinClause, maxJoinClauseLen, ErrInvalidJoinClause); err != nil {
		return err
	}

	if err := validateSuspiciousPatterns(joinClause, commonSuspiciousPatterns); err != nil {
		return err
	}

	return validateJoinFormat(joinClause)
}

func validateJoinFormat(joinClause string) error {
	if !joinPattern.MatchString(strings.TrimSpace(joinClause)) {
		return ErrInvalidJoinClause
	}
	return nil
}

func ValidateColumnName(columnName string) error {
	if len(columnName) == 0 {
		return ErrInvalidColumnName
	}

	if len(columnName) > maxIdentifierLen {
		return ErrIdentifierTooLong
	}

	if !columnNamePattern.MatchString(columnName) {
		return ErrInvalidColumnName
	}

	return nil
}

func ValidateTableName(tableName string) error {
	if len(tableName) == 0 {
		return ErrInvalidIdentifier
	}

	if len(tableName) > maxIdentifierLen {
		return ErrIdentifierTooLong
	}

	if !sqlIdentifierPattern.MatchString(tableName) {
		return ErrInvalidIdentifier
	}

	return nil
}

func ValidateIdentifier(identifier string) error {
	if len(identifier) == 0 {
		return ErrInvalidIdentifier
	}

	if len(identifier) > maxIdentifierLen {
		return ErrIdentifierTooLong
	}

	if !sqlIdentifierPattern.MatchString(identifier) {
		return ErrInvalidIdentifier
	}

	return nil
}

func SanitizeColumnNames(columns []string) ([]string, error) {
	sanitized := make([]string, 0, len(columns))

	for _, col := range columns {
		trimmed := strings.TrimSpace(col)
		if err := ValidateColumnName(trimmed); err != nil {
			return nil, err
		}
		sanitized = append(sanitized, trimmed)
	}

	return sanitized, nil
}

func ValidateWhereClause(whereClause string) error {
	if err := validateLength(whereClause, maxWhereClauseLen, ErrSuspiciousPattern); err != nil {
		return err
	}

	// Check for dangerous patterns in WHERE clauses
	allPatterns := append(commonSuspiciousPatterns, databaseMetadataPatterns...)
	return validateSuspiciousPatterns(whereClause, allPatterns)
}

// Enhanced validation functions
func validateStringParameter(str string) (string, error) {
	if err := ValidateWhereClause(str); err != nil {
		return "", err
	}
	return str, nil
}

func validateStringSliceParameter(slice []string) ([]string, error) {
	for _, str := range slice {
		if err := ValidateWhereClause(str); err != nil {
			return nil, err
		}
	}
	return slice, nil
}

func sanitizeSelectField(field string) (string, error) {
	trimmed := strings.TrimSpace(field)

	// Allow wildcard
	if trimmed == "*" {
		return trimmed, nil
	}

	// Validate table.column format
	return validateTableColumnFormat(trimmed)
}

func validateTableColumnFormat(field string) (string, error) {
	parts := strings.Split(field, ".")
	if len(parts) > 2 {
		return "", ErrInvalidColumnName
	}

	for _, part := range parts {
		if err := ValidateIdentifier(part); err != nil {
			return "", err
		}
	}

	return field, nil
} // Enhanced ValidateAndSanitizeParameter with better type handling
func ValidateAndSanitizeParameter(param interface{}) (interface{}, error) {
	if param == nil {
		return nil, nil
	}

	switch v := param.(type) {
	case string:
		return validateStringParameter(v)
	case []string:
		return validateStringSliceParameter(v)
	case int, int8, int16, int32, int64:
		return v, nil
	case uint, uint8, uint16, uint32, uint64:
		return v, nil
	case float32, float64:
		return v, nil
	case bool:
		return v, nil
	default:
		return param, nil
	}
}

// Validate SQL operator to prevent injection through operators
func ValidateSQLOperator(operator string) error {
	allowedOperators := []string{
		"=", "!=", "<>", "<", ">", "<=", ">=",
		"LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE",
		"IN", "NOT IN", "IS", "IS NOT",
		"BETWEEN", "NOT BETWEEN",
		"AND", "OR", "NOT",
	}

	upperOp := strings.ToUpper(strings.TrimSpace(operator))
	for _, allowed := range allowedOperators {
		if upperOp == allowed {
			return nil
		}
	}

	return ErrSuspiciousPattern
}

// Enhanced SanitizeSelectFields with better validation
func SanitizeSelectFields(fields []string) ([]string, error) {
	if len(fields) == 0 {
		return fields, nil
	}

	sanitized := make([]string, 0, len(fields))
	for _, field := range fields {
		sanitizedField, err := sanitizeSelectField(field)
		if err != nil {
			return nil, err
		}
		sanitized = append(sanitized, sanitizedField)
	}

	return sanitized, nil
} // Validate HAVING clause
func ValidateHavingClause(having []string) error {
	for _, clause := range having {
		if err := ValidateWhereClause(clause); err != nil {
			return err
		}

		// Additional validation for HAVING clauses
		upperClause := strings.ToUpper(clause)
		if !strings.Contains(upperClause, "COUNT") &&
			!strings.Contains(upperClause, "SUM") &&
			!strings.Contains(upperClause, "AVG") &&
			!strings.Contains(upperClause, "MIN") &&
			!strings.Contains(upperClause, "MAX") {
			// HAVING clauses typically use aggregate functions
			// But we'll allow other clauses too for flexibility
		}
	}
	return nil
}

// Safe wrapper functions that enforce validation
func SafeOrderBy(order string) (string, error) {
	if err := ValidateOrderBy(order); err != nil {
		return "", err
	}
	return order, nil
}

func SafeJoinClause(joinClause string) (string, error) {
	if err := ValidateJoinClause(joinClause); err != nil {
		return "", err
	}
	return joinClause, nil
}

func SafeSelectFields(fields []string) ([]string, error) {
	return SanitizeSelectFields(fields)
}

func SafeGroupByFields(fields []string) ([]string, error) {
	return SanitizeColumnNames(fields)
}

func SafeHavingClauses(clauses []string) ([]string, error) {
	if err := ValidateHavingClause(clauses); err != nil {
		return nil, err
	}
	return clauses, nil
}

// Field map cache for performance optimization
type FieldMapCache struct {
	gormCache sync.Map
	sqlCache  sync.Map
}

var fieldMapCache = &FieldMapCache{}

// Cached version of field extraction
func CachedGormTablerAllowedFields(model Tabler) map[string]string {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if cached, ok := fieldMapCache.gormCache.Load(t); ok {
		return cached.(map[string]string)
	}

	fields := DefaultGormTablerAllowedFields(model)
	fieldMapCache.gormCache.Store(t, fields)
	return fields
}

func CachedSqlTablerAllowedFields(model Tabler) map[string]string {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if cached, ok := fieldMapCache.sqlCache.Load(t); ok {
		return cached.(map[string]string)
	}

	fields := DefaultSqlTablerAllowedFields(model)
	fieldMapCache.sqlCache.Store(t, fields)
	return fields
}

// Clear cache when needed
func ClearFieldMapCache() {
	fieldMapCache.gormCache.Range(func(key, value interface{}) bool {
		fieldMapCache.gormCache.Delete(key)
		return true
	})
	fieldMapCache.sqlCache.Range(func(key, value interface{}) bool {
		fieldMapCache.sqlCache.Delete(key)
		return true
	})
}

func applyScopes(a QueryAdapter, fs ...ScopeFunc) QueryAdapter {
	for _, f := range fs {
		a = f(a)
	}
	return a
}

// Helper function to extract field information
func extractFieldInfo(field reflect.StructField) (jsonName string, shouldSkip bool) {
	raw := field.Tag.Get("json")
	if raw == "-" {
		return "", true
	}

	if raw == "" {
		return strings.ToLower(field.Name), false
	}

	// tag may contain options after comma, take name part only
	parts := strings.SplitN(raw, ",", 2)
	name := parts[0]
	if name == "" {
		// empty name means use field name
		return strings.ToLower(field.Name), false
	}
	return name, false
}

// Helper function to extract column name from tag
func extractColumnFromTag(tag, prefix string) string {
	if tag == "" {
		return ""
	}

	for _, part := range strings.Split(tag, ";") {
		if strings.HasPrefix(part, prefix) {
			return strings.TrimPrefix(part, prefix)
		}
	}
	return ""
}

func DefaultGormTablerAllowedFields(model Tabler) map[string]string {
	return extractAllowedFields(model, "gorm")
}

func DefaultSqlTablerAllowedFields(model Tabler) map[string]string {
	return extractAllowedFields(model, "sql")
}

func extractAllowedFields(model Tabler, tagName string) map[string]string {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	fields := make(map[string]string)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		jsonName, columnName := extractFieldMapping(field, tagName)

		if columnName != "" && isValidColumnName(columnName) {
			fields[jsonName] = columnName
		}
	}
	return fields
}

func extractFieldMapping(field reflect.StructField, tagName string) (jsonName, columnName string) {
	jsonName, shouldSkip := extractFieldInfo(field)
	if shouldSkip {
		return "", ""
	}

	tag := field.Tag.Get(tagName)
	columnName = extractColumnFromTag(tag, columnTagPrefix)

	return jsonName, columnName
}

func isValidColumnName(columnName string) bool {
	return columnNamePattern.MatchString(columnName)
}

var debug = false

func DebugOn() {
	debug = true
}

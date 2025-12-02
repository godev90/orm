# Go ORM Library

A secure, high-performance ORM library for Go with built-in SQL injection protection and support for both GORM and native SQL.

## 🚀 Features

### 🔒 **Security First (Auto-Safe by Default)**
- **Automatic SQL Injection Protection**: All standard methods (`Order()`, `Join()`, `Select()`, etc.) now automatically validate and sanitize inputs
- **Backward Compatible**: `Safe*` methods remain available for explicit safety
- **Advanced Control**: `Unsafe*` methods for advanced users who need to bypass validation
- **Input Sanitization**: Automatic sanitization of column names, table names, and query clauses
- **Pattern Detection**: Blocks suspicious SQL patterns and database metadata access

### ⚡ **High Performance**
- **Field Map Caching**: Cached reflection-based field mapping
- **Validation Caching**: Cached validation results for repeated queries
- **Memory Efficient**: Optimized memory usage with cache size limits

### 🔄 **Dual Adapter Support**
- **GORM Adapter**: High-level ORM functionality
- **Native SQL Adapter**: Direct SQL control for performance-critical operations
- **Unified Interface**: Consistent API across both adapters

### 🛠 **Developer Friendly**
- **Zero Configuration Security**: Safety works out of the box without configuration
- **Type Safety**: Strong typing with Go structs
- **Context Support**: Built-in timeout and cancellation support
- **Error Handling**: Comprehensive error types with HTTP status codes
- **Documentation**: Extensive examples and usage patterns

## 📦 Installation

```bash
go get github.com/godev90/orm
```

## 🔧 Quick Start

### Basic Usage with GORM

```go
package main

import (
    "context"
    "time"
    
    "github.com/godev90/orm"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
)

type User struct {
    ID    int    `json:"id" gorm:"column:id"`
    Name  string `json:"name" gorm:"column:name"`
    Email string `json:"email" gorm:"column:email"`
}

func (User) TableName() string {
    return "users"
}

func main() {
    // Connect to database
    db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
    if err != nil {
        panic(err)
    }
    
    // Create adapter
    adapter := orm.NewGormAdapter(db)
    
    // Standard methods now automatically safe - no prefix needed!
    var users []User
    err = adapter.
        UseModel(&User{}).
        Select([]string{"id", "name", "email"}).  // Automatic sanitization
        Order("created_at DESC").                 // Automatic validation
        Where("status = ?", "active").
        Limit(10).
        Scan(&users)
}
```

### Usage with Native SQL

```go
import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
)

func main() {
    // Connect to database
    db, err := sql.Open("mysql", dsn)
    if err != nil {
        panic(err)
    }
    
    // Create adapter
    adapter := orm.NewSqlAdapter(db)
    
    // Standard methods now automatically safe
    var user User
    err = adapter.
        UseModel(&User{}).
        Select([]string{"*"}).  // Automatic sanitization
        Where("email = ?", "user@example.com").
        First(&user)
}
```

## 🛡️ Security Features

### Automatic SQL Injection Protection

All standard query methods now automatically validate and sanitize inputs:

```go
// These methods are now automatically safe - no "Safe" prefix needed!
adapter.Order("name ASC")                                    // ✅ Validated
adapter.Join("INNER JOIN profiles ON users.id = profiles.user_id") // ✅ Validated  
adapter.Select([]string{"id", "name", "email"})            // ✅ Sanitized

// Dangerous inputs are automatically blocked:
adapter.Order("name; DROP TABLE users; --")                // ❌ Blocked
adapter.Join("JOIN users; DELETE FROM posts; --")          // ❌ Blocked
adapter.Select([]string{"*; DROP TABLE users"})            // ❌ Blocked
```

### Backward Compatibility

For users upgrading from previous versions, `Safe*` methods remain available:

```go
// These methods still work (backward compatibility)
adapter.SafeOrder("name ASC")
adapter.SafeJoin("INNER JOIN profiles ON users.id = profiles.user_id")
adapter.SafeSelect([]string{"id", "name", "email"})
```

### Advanced Usage

For advanced users who need to bypass validation:

```go
// Use Unsafe* methods when you need to bypass validation (use with caution!)
adapter.UnsafeOrder(complexOrderClause)
adapter.UnsafeJoin(dynamicJoinClause)
adapter.UnsafeSelect(computedFields)
```

### Input Validation

```go
// Validate parameters before using
param, err := orm.ValidateAndSanitizeParameter(userInput)
if err != nil {
    // Handle validation error
    return
}

// Validate ORDER BY clauses
if err := orm.ValidateOrderBy(orderBy); err != nil {
    // Handle invalid ORDER BY
    return
}

// Validate column names
if err := orm.ValidateColumnName(column); err != nil {
    // Handle invalid column
    return
}
```

## ⚡ Performance Optimization

### Field Map Caching

Use cached field mapping for better performance:

```go
// First call extracts and caches field mapping
fields := orm.CachedGormTablerAllowedFields(&User{})

// Subsequent calls return cached results (much faster)
fields = orm.CachedGormTablerAllowedFields(&User{})

// Clear cache when needed
orm.ClearFieldMapCache()
```

### Context with Timeout

```go
// Set query timeout
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

err := adapter.
    WithContext(ctx).
    Where("complex_condition = ?", value).
    Scan(&results)
```

## 🔧 Advanced Usage

### Transactions

```go
// Create transaction adapter
tx, err := orm.NewSqlTransactionAdapter(ctx, db)
if err != nil {
    return err
}
defer tx.Rollback()

// Create record
if err := tx.Create(&user); err != nil {
    return err
}

// Update record
if err := tx.Update(&user); err != nil {
    return err
}

// Bulk insert
if err := tx.BulkInsert(users); err != nil {
    return err
}

// Commit transaction
return tx.Commit()
```

### Scopes

1) In-place scope (example: paginate)
```go
func Paginate(page, pageSize int) ScopeFunc {
    return func(db QueryAdapter) QueryAdapter {
        if page <= 0 { page = 1 }
        switch {
        case pageSize > 100:
            pageSize = 100
        case pageSize <= 0:
            pageSize = 10
        }
        offset := (page - 1) * pageSize
        return db.Offset(offset).Limit(pageSize)
    }
}
```

2) Isolated scope that builds a grouped WHERE (example: search across multiple columns)
```go
func SearchScope(keyword string, fields []string, allowed map[string]string) ScopeFunc {
    return func(db QueryAdapter) QueryAdapter {
        if keyword == "" || len(fields) == 0 {
            return db
        }
        clone := db.Clone() // build on a clone so parent WHEREs aren't inherited
        for _, f := range fields {
            if col, ok := allowed[f]; ok {
                cond := col + " LIKE ?"
                if db.Driver() == orm.FlavorPostgres {
                    cond = col + " ILIKE ?"
                }
                clone = clone.Or(cond, "%"+keyword+"%")
            }
        }
        // injecting the clone groups the OR conditions: (... OR ... OR ...)
        return db.Where(clone)
    }
}
```

Usage:
```go
db = db.Scopes(Paginate(1, 20), SearchScope("your keyword", []string{"column_a","columbb","column_c"}, allowed))
```

Notes
- Use in-place scopes for pagination, ordering, joins, etc.
- Use clone + db.Where(clone) pattern for isolated grouped conditions (OR blocks).
- If a scope must add joins visible to the parent, either build on a clone and merge joins back into the parent before returning, or have the scope operate directly on the passed adapter (in-place).
- Ensure your Where implementation trims common leading WHEREs when you pass a sub-adapter clone to avoid duplicating parent filters.

## 🧪 Testing

The library includes comprehensive unit tests and benchmarks:

```bash
# Run tests
go test -v ./...

# Run benchmarks
go test -bench=. ./...

# Run with coverage
go test -cover ./...
```

## 📊 Validation Rules

### Supported SQL Operators

```go
// Allowed operators
"=", "!=", "<>", "<", ">", "<=", ">=",
"LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE", 
"IN", "NOT IN", "IS", "IS NOT",
"BETWEEN", "NOT BETWEEN",
"AND", "OR", "NOT"
```

### Blocked Patterns

```go
// Dangerous patterns automatically blocked
"--", "/*", "*/", ";", "UNION", "DELETE", "DROP", 
"INSERT", "UPDATE", "EXEC", "EXECUTE", "DECLARE", 
"SELECT", "CREATE", "ALTER", "TRUNCATE",
"INFORMATION_SCHEMA", "SYS.", "MYSQL.", "PG_"
```

### Length Limits

```go
const (
    maxIdentifierLen  = 64    // Table/column names
    maxOrderByLen     = 256   // ORDER BY clauses  
    maxJoinClauseLen  = 512   // JOIN clauses
    maxWhereClauseLen = 1024  // WHERE clauses
)
```

## 🔍 Error Handling

The library provides specific error types for different validation failures:

```go
var (
    ErrInvalidIdentifier = errors.New("orm: invalid SQL identifier")
    ErrInvalidOrderBy    = errors.New("orm: invalid ORDER BY clause")
    ErrInvalidJoinClause = errors.New("orm: invalid JOIN clause")
    ErrInvalidColumnName = errors.New("orm: invalid column name")
    ErrIdentifierTooLong = errors.New("orm: identifier too long")
    ErrSuspiciousPattern = errors.New("orm: suspicious SQL pattern detected")
    ErrNotFound         = errors.New("orm: record not found")
)
```

## 🚀 Best Practices

1. **Use standard methods** - they are now automatically safe (no "Safe" prefix needed)
2. **Leverage backward compatibility** - existing `Safe*` methods continue to work
3. **Use `Unsafe*` methods sparingly** - only when you need to bypass validation
4. **Use context with timeout** for long-running queries
5. **Cache field mappings** for frequently used models
6. **Validate parameters** if building dynamic queries
7. **Handle errors properly** with specific error types
8. **Use transactions** for data consistency
9. **Set appropriate limits** for pagination

## 📈 Migration Guide

### From Previous Versions

**Old Code (Still Works):**
```go
// Previous version with explicit Safe* methods
adapter.SafeOrder("created_at DESC").SafeSelect([]string{"id", "name"})
```

**New Code (Recommended):**
```go
// New version with automatic safety
adapter.Order("created_at DESC").Select([]string{"id", "name"})
```

Both approaches provide the same level of security, but the new approach is cleaner and more intuitive.

## 📈 Performance Benchmarks

```bash
BenchmarkValidateOrderBy-8                  5000000    230 ns/op
BenchmarkSanitizeSelectFields-8             2000000    650 ns/op  
BenchmarkCachedGormTablerAllowedFields-8   10000000    120 ns/op
BenchmarkDefaultGormTablerAllowedFields-8    500000   2840 ns/op
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

- **Documentation**: See examples in `examples.go`
- **Issues**: Report bugs on GitHub Issues
- **Security**: Report security issues privately

## 🏆 Features Roadmap

- [ ] Query builder with method chaining
- [ ] Database migration support
- [ ] Soft delete functionality
- [ ] Model hooks and callbacks
- [ ] Connection pooling configuration
- [ ] Query logging and metrics
- [ ] Schema validation

---

**Made with ❤️ for secure Go applications**
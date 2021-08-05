package mysql

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"github.com/xooooooox/gas/sort"
	"strings"
)

// Insert mysql insert map(key-value)
func Insert(table string, insert map[string]interface{}) (rowsAffected int64, err error) {
	if insert == nil || len(insert) == 0 {
		err = errors.New("`insert` is empty")
		return
	}
	var ba, bb, bc bytes.Buffer
	bb.WriteString(fmt.Sprintf("INSERT INTO %s (", Ordinary(table)))
	col, val := sort.AscMsi(insert)
	length := len(col)
	for i := 0; i < length; i++ {
		if i > 0 {
			ba.WriteString(",")
			bc.WriteString(",")
		}
		bc.WriteString(fmt.Sprintf(" %s", Ordinary(col[i])))
		ba.WriteString(fmt.Sprintf(" %s", Placeholder))
	}
	bb.Write(bc.Bytes())
	bb.WriteString(") VALUES (")
	bb.Write(ba.Bytes())
	bb.WriteString(");")
	rowsAffected, err = Exec(bb.String(), val...)
	return
}

// Delete mysql delete where
func Delete(table string, where string, args ...interface{}) (rowsAffected int64, err error) {
	var bb bytes.Buffer
	bb.WriteString(fmt.Sprintf("DELETE FROM %s", Ordinary(table)))
	if where != "" {
		bb.WriteString(fmt.Sprintf(" WHERE (%s)", where))
	}
	bb.WriteString(";")
	rowsAffected, err = Exec(bb.String(), args...)
	return
}

// Update mysql update
func Update(table string, update map[string]interface{}, where string, args ...interface{}) (rowsAffected int64, err error) {
	if update == nil || len(update) == 0 {
		err = errors.New("`update` is empty")
		return
	}
	var bb bytes.Buffer
	bb.WriteString(fmt.Sprintf("UPDATE %s SET", Ordinary(table)))
	col, val := sort.AscMsi(update)
	length := len(col)
	for i := 0; i < length; i++ {
		if i > 0 {
			bb.WriteString(",")
		}
		bb.WriteString(fmt.Sprintf(" %s = %s", Ordinary(col[i]), Placeholder))
	}
	if where != "" {
		bb.WriteString(fmt.Sprintf(" WHERE (%s)", where))
		val = append(val, args...)
	}
	bb.WriteString(";")
	rowsAffected, err = Exec(bb.String(), val...)
	return
}

// AskInsert transaction mysql insert map(key-value)
func AskInsert(tx *sql.Tx, table string, insert map[string]interface{}) (rowsAffected int64, err error) {
	if tx == nil {
		err = errors.New("`tx` is nil")
		return
	}
	if insert == nil || len(insert) == 0 {
		err = errors.New("`insert` is empty")
		return
	}
	var ba, bb, bc bytes.Buffer
	bb.WriteString(fmt.Sprintf("INSERT INTO %s (", Ordinary(table)))
	col, val := sort.AscMsi(insert)
	length := len(col)
	for i := 0; i < length; i++ {
		if i > 0 {
			ba.WriteString(",")
			bc.WriteString(",")
		}
		bc.WriteString(fmt.Sprintf(" %s", Ordinary(col[i])))
		ba.WriteString(fmt.Sprintf(" %s", Placeholder))
	}
	bb.Write(bc.Bytes())
	bb.WriteString(") VALUES (")
	bb.Write(ba.Bytes())
	bb.WriteString(");")
	rowsAffected, err = AskExec(tx, bb.String(), val...)
	return
}

// AskDelete transaction mysql delete where
func AskDelete(tx *sql.Tx, table string, where string, args ...interface{}) (rowsAffected int64, err error) {
	if tx == nil {
		err = errors.New("`tx` is nil")
		return
	}
	var bb bytes.Buffer
	bb.WriteString(fmt.Sprintf("DELETE FROM %s", Ordinary(table)))
	if where != "" {
		bb.WriteString(fmt.Sprintf(" WHERE (%s)", where))
	}
	bb.WriteString(";")
	rowsAffected, err = AskExec(tx, bb.String(), args...)
	return
}

// AskUpdate transaction mysql update
func AskUpdate(tx *sql.Tx, table string, update map[string]interface{}, where string, args ...interface{}) (rowsAffected int64, err error) {
	if tx == nil {
		err = errors.New("`tx` is nil")
		return
	}
	if update == nil || len(update) == 0 {
		err = errors.New("`update` is empty")
		return
	}
	var bb bytes.Buffer
	bb.WriteString(fmt.Sprintf("UPDATE %s SET", Ordinary(table)))
	col, val := sort.AscMsi(update)
	length := len(col)
	for i := 0; i < length; i++ {
		if i > 0 {
			bb.WriteString(",")
		}
		bb.WriteString(fmt.Sprintf(" %s = %s", Ordinary(col[i]), Placeholder))
	}
	if where != "" {
		bb.WriteString(fmt.Sprintf(" WHERE (%s)", where))
		val = append(val, args...)
	}
	bb.WriteString(";")
	rowsAffected, err = AskExec(tx, bb.String(), val...)
	return
}

// Ordinary ordinary mysql string
func Ordinary(s string) string {
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "`", "")
	s = strings.ReplaceAll(s, ".", "`.`")
	return fmt.Sprintf("`%s`", s)
}

// ColumnSingleOperator mysql column single operator
func ColumnSingleOperator(column, operator string) string {
	return fmt.Sprintf("%s %s %s", Ordinary(column), operator, Placeholder)
}

// ColumnEqual mysql column equal
func ColumnEqual(column string) string {
	return ColumnSingleOperator(column, "=")
}

// ColumnNotEqual mysql column not equal
func ColumnNotEqual(column string) string {
	return ColumnSingleOperator(column, "<>")
}

// ColumnGreaterThan mysql column greater than
func ColumnGreaterThan(column string) string {
	return ColumnSingleOperator(column, ">")
}

// ColumnLessThan mysql column less than
func ColumnLessThan(column string) string {
	return ColumnSingleOperator(column, "<")
}

// ColumnGreaterThanOrEqual mysql column greater than or equal
func ColumnGreaterThanOrEqual(column string) string {
	return ColumnSingleOperator(column, ">=")
}

// ColumnLessThanOrEqual mysql column less than or equal
func ColumnLessThanOrEqual(column string) string {
	return ColumnSingleOperator(column, "<=")
}

// ColumnIsNull mysql column is null
func ColumnIsNull(column string) string {
	return fmt.Sprintf("%s IS NULL", Ordinary(column))
}

// ColumnIsNotNull mysql column is not null
func ColumnIsNotNull(column string) string {
	return fmt.Sprintf("%s IS NOT NULL", Ordinary(column))
}

// ColumnBetween mysql column between
func ColumnBetween(column string) string {
	return fmt.Sprintf("%s BETWEEN %s AND %s", Ordinary(column), Placeholder, Placeholder)
}

// ColumnNotBetween mysql column not between
func ColumnNotBetween(column string) string {
	return fmt.Sprintf("%s NOT BETWEEN %s AND %s", Ordinary(column), Placeholder, Placeholder)
}

// ColumnLike mysql column like
func ColumnLike(column string) string {
	return fmt.Sprintf("%s LIKE %s", Ordinary(column), Placeholder)
}

// ColumnNotLike mysql column not like
func ColumnNotLike(column string) string {
	return fmt.Sprintf("%s NOT LIKE %s", Ordinary(column), Placeholder)
}

// ColumnIn mysql column in
func ColumnIn(column string, length int) string {
	p := make([]string, length, length)
	for i := 0; i < length; i++ {
		p[i] = Placeholder
	}
	return fmt.Sprintf("%s IN (%s)", Ordinary(column), strings.Join(p, ", "))
}

// ColumnNotIn mysql column not in
func ColumnNotIn(column string, length int) string {
	p := make([]string, length, length)
	for i := 0; i < length; i++ {
		p[i] = Placeholder
	}
	return fmt.Sprintf("%s NOT IN (%s)", Ordinary(column), strings.Join(p, ", "))
}

// PageLimit mysql page limit
func PageLimit(page, limit int64) string {
	if page <= 0 {
		page = 1
	}
	if limit <= 0 {
		limit = 1
	}
	if limit > 1000 {
		limit = 1000
	}
	return fmt.Sprintf("%d, %d", (page-1)*limit, limit)
}

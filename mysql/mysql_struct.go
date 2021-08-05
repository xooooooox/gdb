// export all table structures of a mysql database

package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/xooooooox/gas/name"
)

const (
	DqlDatabaseTables       = "SELECT `TABLE_SCHEMA`, `TABLE_NAME`, `TABLE_TYPE`, `ENGINE`, `ROW_FORMAT`, `TABLE_ROWS`, `AUTO_INCREMENT`, `CREATE_TIME`, `UPDATE_TIME`, `TABLE_COLLATION`, `TABLE_COMMENT` FROM `information_schema`.`TABLES` WHERE ( `TABLE_SCHEMA` = ? AND `TABLE_TYPE` = 'BASE TABLE' ) ORDER BY `TABLE_NAME` ASC;"
	DqlDatabaseTableColumns = "SELECT `TABLE_SCHEMA`, `TABLE_NAME`, `COLUMN_NAME`, `ORDINAL_POSITION`, `COLUMN_DEFAULT`, `IS_NULLABLE`, `DATA_TYPE`, `CHARACTER_MAXIMUM_LENGTH`, `CHARACTER_OCTET_LENGTH`, `NUMERIC_PRECISION`, `NUMERIC_SCALE`, `CHARACTER_SET_NAME`, `COLLATION_NAME`, `COLUMN_TYPE`, `COLUMN_KEY`, `EXTRA`, `COLUMN_COMMENT` FROM `information_schema`.`COLUMNS` WHERE ( `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ? ) ORDER BY `ORDINAL_POSITION` ASC;"
)

// Table mysql table
type Table struct {
	TableSchema    *string // 数据库名
	TableName      *string // 表名
	TableType      *string // 表类型
	Engine         *string // 表存储引擎
	RowFormat      *string // 行格式
	TableRows      *int    // 表已经存储数据的行数
	AutoIncrement  *int    // 自动递增值
	CreateTime     *string // 创建时间
	UpdateTime     *string // 更新时间
	TableCollation *string // 校对集名称
	TableComment   *string // 表注释
}

// Tables query mysql table
func Tables(database string) (bag []*Table, err error) {
	fc := func(rows *sql.Rows) (err error) {
		for rows.Next() {
			s := &Table{}
			err = rows.Scan(
				&s.TableSchema,
				&s.TableName,
				&s.TableType,
				&s.Engine,
				&s.RowFormat,
				&s.TableRows,
				&s.AutoIncrement,
				&s.CreateTime,
				&s.UpdateTime,
				&s.TableCollation,
				&s.TableComment,
			)
			if err != nil {
				break
			}
			bag = append(bag, s)
		}
		return
	}
	err = Query(fc, DqlDatabaseTables, database)
	return
}

// Column mysql column
type Column struct {
	TableSchema            *string // 数据库名
	TableName              *string // 表名
	ColumnName             *string // 列名
	OrdinalPosition        *int    // 列序号
	ColumnDefault          *string // 列默认值
	IsNullable             *string // 是否允许列值为null
	DataType               *string // 列数据类型
	CharacterMaximumLength *int    // 字符串最长长度
	CharacterOctetLength   *int    // 字符串字节最长长度
	NumericPrecision       *int    // 整数最长长度|小数(整数+小数)合计长度
	NumericScale           *int    // 小数精度长度
	CharacterSetName       *string // 字符集名称
	CollationName          *string // 校对集名称
	ColumnType             *string // 列类型
	ColumnKey              *string // 列索引
	Extra                  *string // 额外值(主键: PRI)
	ColumnComment          *string // 列注释
}

// Columns query mysql column
func Columns(database, table string) (bag []*Column, err error) {
	fc := func(rows *sql.Rows) (err error) {
		for rows.Next() {
			s := &Column{}
			err = rows.Scan(
				&s.TableSchema,
				&s.TableName,
				&s.ColumnName,
				&s.OrdinalPosition,
				&s.ColumnDefault,
				&s.IsNullable,
				&s.DataType,
				&s.CharacterMaximumLength,
				&s.CharacterOctetLength,
				&s.NumericPrecision,
				&s.NumericScale,
				&s.CharacterSetName,
				&s.CollationName,
				&s.ColumnType,
				&s.ColumnKey,
				&s.Extra,
				&s.ColumnComment,
			)
			if err != nil {
				break
			}
			bag = append(bag, s)
		}
		return
	}
	err = Query(fc, DqlDatabaseTableColumns, database, table)
	return
}

// ColumnToGoType mysql column type to go type
func ColumnToGoType(c *Column) (ts string) {
	nullable := true
	if c.IsNullable != nil && strings.ToLower(*c.IsNullable) == "no" {
		nullable = false
	}
	datatype := ""
	if c.DataType != nil {
		datatype = strings.ToLower(*c.DataType)
	}
	ct := ""
	if c.ColumnType != nil {
		ct = strings.ToLower(*c.ColumnType)
	}
	if strings.Index(datatype, "int") >= 0 {
		// int
		if strings.Index(ct, "unsigned") >= 0 {
			if strings.Index(datatype, "tinyint") >= 0 {
				ts = "uint8"
			} else if strings.Index(datatype, "smallint") >= 0 {
				ts = "uint16"
			} else if strings.Index(datatype, "mediumint") >= 0 || strings.Index(datatype, "integer") >= 0 {
				ts = "uint"
			} else if strings.Index(datatype, "bigint") >= 0 {
				ts = "uint64"
			} else {
				ts = "uint"
			}
		} else {
			if strings.Index(datatype, "tinyint") >= 0 {
				ts = "int8"
			} else if strings.Index(datatype, "smallint") >= 0 {
				ts = "int16"
			} else if strings.Index(datatype, "mediumint") >= 0 || strings.Index(datatype, "integer") >= 0 {
				ts = "int"
			} else if strings.Index(datatype, "bigint") >= 0 {
				ts = "int64"
			} else {
				ts = "int"
			}
		}
	} else if strings.Index(datatype, "float") >= 0 || strings.Index(datatype, "double") >= 0 || strings.Index(datatype, "decimal") >= 0 || strings.Index(datatype, "numeric") >= 0 {
		// 近似值 float double; 精确值 decimal,numeric
		ts = "float64"
	} else if datatype == "varchar" {
		ts = "string"
	} else if datatype == "char" {
		ts = "string"
	} else if strings.Index(datatype, "binary") >= 0 || strings.Index(datatype, "varbinary") >= 0 {
		ts = "[]byte"
	} else if strings.Index(datatype, "text") >= 0 {
		ts = "string"
	} else if strings.Index(datatype, "blob") >= 0 {
		ts = "[]byte"
	} else if strings.Index(datatype, "enum") >= 0 || strings.Index(datatype, "set") >= 0 {
		ts = "string"
	} else {
		ts = "string"
	}
	if nullable && ts != "" && strings.Index(ts, "byte") < 0 {
		ts = "*" + ts
	}
	return
}

// FmtGoFile fmt go source file
func FmtGoFile(file string) error {
	cmd := exec.Command("go", "fmt", file)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// WriteDatabase write database
type WriteDatabase struct {
	DatabaseName string
	Table        []*WriteTable
}

// WriteTable write table
type WriteTable struct {
	TableName          string
	TableComment       string
	TableNamePascal    string
	TableNameUnderline string
	Column             []*WriteColumn
}

// WriteColumn write column
type WriteColumn struct {
	ColumnName          string
	ColumnComment       string
	ColumnNamePascal    string
	ColumnNameUnderline string
	DateType            string
}

func AscMapStringString(msi map[string]string) (key []string, val []string) {
	if msi == nil {
		return
	}
	length := len(msi)
	key = make([]string, length, length)
	val = make([]string, length, length)
	i := 0
	for k := range msi {
		key[i] = k
		i++
	}
	sort.Strings(key)
	for k, v := range key {
		val[k] = msi[v]
	}
	return
}

// DatabaseTableColumnsToString all columns to string
func DatabaseTableColumnsToString(columns []*Column, coated string) string {
	var cs []string
	for _, c := range columns {
		if c.ColumnName == nil {
			continue
		}
		cs = append(cs, fmt.Sprintf("%s%s%s", coated, *c.ColumnName, coated))
	}
	return strings.Join(cs, ", ")
}

// DatabaseTableColumnsToScanString all columns to scan string
func DatabaseTableColumnsToScanString(columns []*Column) string {
	var cs bytes.Buffer
	length := len(columns)
	for i := 0; i < length; i++ {
		if columns[i].ColumnName == nil {
			continue
		}
		cs.WriteString(fmt.Sprintf("\t\t\t\t&s.%s,", name.UnderlineToPascal(strings.ToLower(*columns[i].ColumnName))))
		if i < length-1 {
			cs.WriteString("\n")
		}
	}
	return cs.String()
}

func WriteDatabaseToGoStruct(database, filename, pkg string) (err error) {
	var tables []*Table
	tables, err = Tables(database)
	if err != nil {
		return
	}
	wd := WriteDatabase{
		DatabaseName: database,
		Table:        []*WriteTable{},
	}
	var bbs, ut, cs, uc bytes.Buffer
	bbs.WriteString(fmt.Sprintf("package %s\n\n", pkg))
	ut.WriteString(fmt.Sprintf("const (\n"))
	cs.WriteString(fmt.Sprintf("const (\n"))
	uc.WriteString(fmt.Sprintf("const (\n"))
	uniqueColumn := map[string]string{}
	// query table.columns, assemble structure data
	for _, t := range tables {
		if t.TableName == nil {
			continue
		}
		wt := &WriteTable{}
		if t.TableComment != nil {
			wt.TableComment = *t.TableComment
		}
		flt := strings.ToLower(*t.TableName)
		wt.TableNamePascal = name.UnderlineToPascal(flt)
		wt.TableNameUnderline = name.PascalToUnderline(wt.TableNamePascal)
		ut.WriteString(fmt.Sprintf("\tUt%s = \"%s\" // %s\n", wt.TableNamePascal, wt.TableNameUnderline, wt.TableComment))
		var columns []*Column
		columns, err = Columns(database, *t.TableName)
		if err != nil {
			continue
		}
		cs.WriteString(fmt.Sprintf("Cs%s = \"%s\" // %s\n", wt.TableNamePascal, DatabaseTableColumnsToString(columns, "`"), wt.TableComment))
		for _, c := range columns {
			if c.ColumnName == nil {
				continue
			}
			wc := &WriteColumn{
				ColumnName: *c.ColumnName,
				DateType:   "",
			}
			if c.ColumnComment != nil {
				wc.ColumnComment = *c.ColumnComment
			}
			flc := strings.ToLower(wc.ColumnName)
			wc.ColumnNamePascal = name.UnderlineToPascal(flc)
			wc.ColumnNameUnderline = name.PascalToUnderline(wc.ColumnNamePascal)
			// bd.WriteString(fmt.Sprintf("\t%s%s = \"%s\" // %s.%s\n", wt.TableNamePascal, wc.ColumnNamePascal, wc.ColumnNameUnderline, wt.TableComment, wc.ColumnComment))
			uniqueColumn[wc.ColumnNamePascal] = *c.ColumnName
			gdv := ""
			if c.ColumnDefault == nil {
				gdv = "nil"
			} else {
				gdv = strings.ToLower(fmt.Sprintf("%v", *c.ColumnDefault))
			}
			if gdv == "null" {
				gdv = "nil"
			}
			if gdv == "" {
				gdv = "\"\""
			}
			if gdv == "''" {
				gdv = "\"\""
			}
			wc.DateType = ColumnToGoType(c)
			wt.Column = append(wt.Column, wc)
		}
		wd.Table = append(wd.Table, wt)
	}
	uk, uv := AscMapStringString(uniqueColumn)
	length := len(uk)
	for i := 0; i < length; i++ {
		uc.WriteString(fmt.Sprintf("\tUc%s = \"%s\"\n", uk[i], uv[i]))
	}
	ut.WriteString(")\n\n")
	cs.WriteString(")\n\n")
	uc.WriteString(")\n\n")
	bbs.Write(ut.Bytes())
	bbs.Write(cs.Bytes())
	bbs.Write(uc.Bytes())
	for _, t := range wd.Table {
		bbs.WriteString(fmt.Sprintf("// %s %s %s\n", t.TableNamePascal, t.TableNameUnderline, t.TableComment))
		bbs.WriteString(fmt.Sprintf("type %s struct {\n", t.TableNamePascal))
		for _, c := range t.Column {
			bbs.WriteString(fmt.Sprintf("\t%s %s `json:\"%s\"` // %s\n", c.ColumnNamePascal, c.DateType, c.ColumnNameUnderline, c.ColumnComment))
		}
		bbs.WriteString("}\n\n")
	}
	if filename == "" {
		filename = "database.go"
	}
	err = os.WriteFile(filename, bbs.Bytes(), 0644)
	if err != nil {
		return
	}
	err = FmtGoFile(filename)
	if err != nil {
		return
	}
	return
}

func WriteDatabaseToGoMap(database, filename string) (err error) {
	var tables []*Table
	tables, err = Tables(database)
	if err != nil {
		return
	}
	var assoc bytes.Buffer
	// query table.columns, assemble structure data
	for _, t := range tables {
		if t.TableName == nil {
			continue
		}
		wt := &WriteTable{}
		if t.TableComment != nil {
			wt.TableComment = *t.TableComment
		}
		flt := strings.ToLower(*t.TableName)
		wt.TableNamePascal = name.UnderlineToPascal(flt)
		wt.TableNameUnderline = name.PascalToUnderline(wt.TableNamePascal)
		assoc.WriteString(fmt.Sprintf("%s %s %s\n", wt.TableNamePascal, wt.TableNameUnderline, wt.TableComment))
		var columns []*Column
		columns, err = Columns(database, *t.TableName)
		if err != nil {
			continue
		}
		assoc.WriteString(fmt.Sprintf("map[string]interface{}{\n"))
		for _, c := range columns {
			if c.ColumnName == nil {
				continue
			}
			wc := &WriteColumn{
				ColumnName: *c.ColumnName,
				DateType:   "",
			}
			if c.ColumnComment != nil {
				wc.ColumnComment = *c.ColumnComment
			}
			flc := strings.ToLower(wc.ColumnName)
			wc.ColumnNamePascal = name.UnderlineToPascal(flc)
			wc.ColumnNameUnderline = name.PascalToUnderline(wc.ColumnNamePascal)
			gdv := ""
			if c.ColumnDefault == nil {
				gdv = "nil"
			} else {
				gdv = strings.ToLower(fmt.Sprintf("%v", *c.ColumnDefault))
			}
			if gdv == "null" {
				gdv = "nil"
			}
			if gdv == "" {
				gdv = "\"\""
			}
			if gdv == "''" {
				gdv = "\"\""
			}
			assoc.WriteString(fmt.Sprintf("\t\"%s\": %s,\n", wc.ColumnName, gdv))
		}
		assoc.WriteString(fmt.Sprintf("}\n\n"))
	}
	if filename == "" {
		filename = "column.map"
	}
	err = os.WriteFile(filename, assoc.Bytes(), 0644)
	if err != nil {
		return
	}
	return
}

func WriteDatabaseToGoSlice(database, filename string) (err error) {
	var tables []*Table
	tables, err = Tables(database)
	if err != nil {
		return
	}
	var assoc bytes.Buffer
	// query table.columns, assemble structure data
	for _, t := range tables {
		if t.TableName == nil {
			continue
		}
		wt := &WriteTable{}
		if t.TableComment != nil {
			wt.TableComment = *t.TableComment
		}
		flt := strings.ToLower(*t.TableName)
		wt.TableNamePascal = name.UnderlineToPascal(flt)
		wt.TableNameUnderline = name.PascalToUnderline(wt.TableNamePascal)
		assoc.WriteString(fmt.Sprintf("%s %s %s\n", wt.TableNamePascal, wt.TableNameUnderline, wt.TableComment))
		var columns []*Column
		columns, err = Columns(database, *t.TableName)
		if err != nil {
			continue
		}
		for _, c := range columns {
			if c.ColumnName == nil {
				continue
			}
			wc := &WriteColumn{
				ColumnName: *c.ColumnName,
				DateType:   "",
			}
			if c.ColumnComment != nil {
				wc.ColumnComment = *c.ColumnComment
			}
			flc := strings.ToLower(wc.ColumnName)
			wc.ColumnNamePascal = name.UnderlineToPascal(flc)
			wc.ColumnNameUnderline = name.PascalToUnderline(wc.ColumnNamePascal)
			assoc.WriteString(fmt.Sprintf("\t\"%s\",\n", wc.ColumnName))
		}
		assoc.WriteString("\n")
	}
	if filename == "" {
		filename = "column.slice"
	}
	err = os.WriteFile(filename, assoc.Bytes(), 0644)
	if err != nil {
		return
	}
	return
}

func WriteDatabaseToGoScan(database, filename string) (err error) {
	var tables []*Table
	tables, err = Tables(database)
	if err != nil {
		return
	}
	var assoc bytes.Buffer
	// query table.columns, assemble structure data
	for _, t := range tables {
		if t.TableName == nil {
			continue
		}
		wt := &WriteTable{}
		if t.TableComment != nil {
			wt.TableComment = *t.TableComment
		}
		flt := strings.ToLower(*t.TableName)
		wt.TableNamePascal = name.UnderlineToPascal(flt)
		wt.TableNameUnderline = name.PascalToUnderline(wt.TableNamePascal)
		assoc.WriteString(fmt.Sprintf("%s %s %s\n", wt.TableNamePascal, wt.TableNameUnderline, wt.TableComment))
		var columns []*Column
		columns, err = Columns(database, *t.TableName)
		if err != nil {
			continue
		}
		for _, c := range columns {
			if c.ColumnName == nil {
				continue
			}
			wc := &WriteColumn{
				ColumnName: *c.ColumnName,
				DateType:   "",
			}
			if c.ColumnComment != nil {
				wc.ColumnComment = *c.ColumnComment
			}
			flc := strings.ToLower(wc.ColumnName)
			wc.ColumnNamePascal = name.UnderlineToPascal(flc)
			wc.ColumnNameUnderline = name.PascalToUnderline(wc.ColumnNamePascal)
			assoc.WriteString(fmt.Sprintf("\t&s.%s,", wc.ColumnNamePascal))
			assoc.WriteString("\n")
		}
		assoc.WriteString("\n")
	}
	if filename == "" {
		filename = "column"
	}
	err = os.WriteFile(filename, assoc.Bytes(), 0644)
	if err != nil {
		return
	}
	return
}

func WriteDatabaseToGoColumn(database, filename string) (err error) {
	var tables []*Table
	tables, err = Tables(database)
	if err != nil {
		return
	}
	var assoc bytes.Buffer
	// query table.columns, assemble structure data
	for _, t := range tables {
		if t.TableName == nil {
			continue
		}
		wt := &WriteTable{}
		if t.TableComment != nil {
			wt.TableComment = *t.TableComment
		}
		flt := strings.ToLower(*t.TableName)
		wt.TableNamePascal = name.UnderlineToPascal(flt)
		wt.TableNameUnderline = name.PascalToUnderline(wt.TableNamePascal)
		assoc.WriteString(fmt.Sprintf("%s %s %s\n", wt.TableNamePascal, wt.TableNameUnderline, wt.TableComment))
		var columns []*Column
		columns, err = Columns(database, *t.TableName)
		if err != nil {
			continue
		}
		assoc.WriteString(DatabaseTableColumnsToString(columns, ""))
		assoc.WriteString("\n")
		assoc.WriteString(DatabaseTableColumnsToString(columns, "`"))
		assoc.WriteString("\n")
		assoc.WriteString(DatabaseTableColumnsToString(columns, "'"))
		assoc.WriteString("\n")
		assoc.WriteString(DatabaseTableColumnsToString(columns, "\""))
		assoc.WriteString("\n")
		assoc.WriteString("\n")
	}
	if filename == "" {
		filename = "column"
	}
	err = os.WriteFile(filename, assoc.Bytes(), 0644)
	if err != nil {
		return
	}
	return
}

func WriteDatabaseQuicklyQuery(database, filename, pkg string) (err error) {
	var assoc bytes.Buffer
	tmp :=
		`package %s

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/xooooooox/gdb/mysql"
)
`
	assoc.WriteString(fmt.Sprintf(tmp, pkg))
	var tables []*Table
	tables, err = Tables(database)
	if err != nil {
		return
	}
	tmpFirst :=
		`
// %sFirst query the first record of where
func %sFirst(where string, args ...interface{}) (s *%s, err error) {
	rows := func(rows *sql.Rows) (err error) {
		if rows.Next() {
			s = &%s{}
			err = rows.Scan(
%s
			)
			if err != nil {
				s = nil
				return
			}
		}
		return
	}
	var prepare bytes.Buffer
	prepare.WriteString(fmt.Sprintf("%s", where))
	err = mysql.Query(rows, prepare.String(), args...)
	return
}
`
	tmpPage :=
		`
// %sPage query one page record of where
func %sPage(page int64, limit int64, where string, args ...interface{}) (ss []*%s, err error) {
	rows := func(rows *sql.Rows) (err error) {
		for rows.Next() {
			s := &%s{}
			err = rows.Scan(
%s
			)
			if err != nil {
				ss = nil
				return
			}
			ss = append(ss, s)
		}
		return
	}
	var prepare bytes.Buffer
	prepare.WriteString(fmt.Sprintf("%s", where))
	prepare.WriteString(fmt.Sprintf(" LIMIT %%s;", mysql.PageLimit(page, limit)))
	err = mysql.Query(rows, prepare.String(), args...)
	return
}
`
	tmpInsert :=
		`
// %sInsert execute insert one record
func %sInsert(insert map[string]interface{}) (rowsAffected int64, err error) {
	return mysql.Insert("%s", insert)
}
`
	tmpDelete :=
		`
// %sDelete execute delete
func %sDelete(where string, args ...interface{}) (rowsAffected int64, err error) {
	return mysql.Delete("%s", where, args...)
}
`
	tmpUpdate :=
		`
// %sUpdate execute update
func %sUpdate(update map[string]interface{}, where string, args ...interface{}) (rowsAffected int64, err error) {
	return mysql.Update("%s", update, where, args...)
}
`
	tmpCount :=
		`
// %sCount Count the number of eligible data
func %sCount(where string, args ...interface{}) (count int64, err error) {
	rows := func(rows *sql.Rows) (err error) {
		if rows.Next() {
			var s *int64
			err = rows.Scan(&s)
			if err != nil {
				return
			}
			count = *s
		}
		return
	}
	var prepare bytes.Buffer
	prepare.WriteString(fmt.Sprintf("%s", where))
	err = mysql.Query(rows, prepare.String(), args...)
	return
}
`
	tmpSumInt :=
		`
// %sSumInt Sum the number of eligible data
func %sSumInt(column string, where string, args ...interface{}) (sum int64, err error) {
	rows := func(rows *sql.Rows) (err error) {
		if rows.Next() {
			var s *int64
			err = rows.Scan(&s)
			if err != nil {
				return
			}
			sum = *s
		}
		return
	}
	var prepare bytes.Buffer
	prepare.WriteString(fmt.Sprintf("%s", column, where))
	err = mysql.Query(rows, prepare.String(), args...)
	return
}
`
	tmpSumFloat :=
		`
// %sSumFloat Sum the number of eligible data
func %sSumFloat(column string, where string, args ...interface{}) (sum float64, err error) {
	rows := func(rows *sql.Rows) (err error) {
		if rows.Next() {
			var s *float64
			err = rows.Scan(&s)
			if err != nil {
				return
			}
			sum = *s
		}
		return
	}
	var prepare bytes.Buffer
	prepare.WriteString(fmt.Sprintf("%s", column, where))
	err = mysql.Query(rows, prepare.String(), args...)
	return
}
`

	tmpAskFirst :=
		`
// %sAskFirst query the first record of where
func %sAskFirst(ask *sql.Tx, where string, args ...interface{}) (s *%s, err error) {
	rows := func(rows *sql.Rows) (err error) {
		if rows.Next() {
			s = &%s{}
			err = rows.Scan(
%s
			)
			if err != nil {
				s = nil
				return
			}
		}
		return
	}
	var prepare bytes.Buffer
	prepare.WriteString(fmt.Sprintf("%s", where))
	err = mysql.AskQuery(ask, rows, prepare.String(), args...)
	return
}
`
	tmpAskPage :=
		`
// %sAskPage query one page record of where
func %sAskPage(ask *sql.Tx, page int64, limit int64, where string, args ...interface{}) (ss []*%s, err error) {
	rows := func(rows *sql.Rows) (err error) {
		for rows.Next() {
			s := &%s{}
			err = rows.Scan(
%s
			)
			if err != nil {
				ss = nil
				return
			}
			ss = append(ss, s)
		}
		return
	}
	var prepare bytes.Buffer
	prepare.WriteString(fmt.Sprintf("%s", where))
	prepare.WriteString(fmt.Sprintf(" LIMIT %%s;", mysql.PageLimit(page, limit)))
	err = mysql.AskQuery(ask, rows, prepare.String(), args...)
	return
}
`
	tmpAskInsert :=
		`
// %sAskInsert execute insert one record
func %sAskInsert(ask *sql.Tx, insert map[string]interface{}) (rowsAffected int64, err error) {
	return mysql.AskInsert(ask, "%s", insert)
}
`
	tmpAskDelete :=
		`
// %sAskDelete execute delete
func %sAskDelete(ask *sql.Tx, where string, args ...interface{}) (rowsAffected int64, err error) {
	return mysql.AskDelete(ask, "%s", where, args...)
}
`
	tmpAskUpdate :=
		`
// %sAskUpdate execute update
func %sAskUpdate(ask *sql.Tx, update map[string]interface{}, where string, args ...interface{}) (rowsAffected int64, err error) {
	return mysql.AskUpdate(ask, "%s", update, where, args...)
}
`
	tmpAskCount :=
		`
// %sAskCount Count the number of eligible data
func %sAskCount(ask *sql.Tx, where string, args ...interface{}) (count int64, err error) {
	rows := func(rows *sql.Rows) (err error) {
		if rows.Next() {
			var s *int64
			err = rows.Scan(&s)
			if err != nil {
				return
			}
			count = *s
		}
		return
	}
	var prepare bytes.Buffer
	prepare.WriteString(fmt.Sprintf("%s", where))
	err = mysql.AskQuery(ask, rows, prepare.String(), args...)
	return
}
`
	tmpAskSumInt :=
		`
// %sAskSumInt Sum the number of eligible data
func %sAskSumInt(ask *sql.Tx, column string, where string, args ...interface{}) (sum int64, err error) {
	rows := func(rows *sql.Rows) (err error) {
		if rows.Next() {
			var s *int64
			err = rows.Scan(&s)
			if err != nil {
				return
			}
			sum = *s
		}
		return
	}
	var prepare bytes.Buffer
	prepare.WriteString(fmt.Sprintf("%s", column, where))
	err = mysql.AskQuery(ask, rows, prepare.String(), args...)
	return
}
`
	tmpAskSumFloat :=
		`
// %sAskSumFloat Sum the number of eligible data
func %sAskSumFloat(ask *sql.Tx, column string, where string, args ...interface{}) (sum float64, err error) {
	rows := func(rows *sql.Rows) (err error) {
		if rows.Next() {
			var s *float64
			err = rows.Scan(&s)
			if err != nil {
				return
			}
			sum = *s
		}
		return
	}
	var prepare bytes.Buffer
	prepare.WriteString(fmt.Sprintf("%s", column, where))
	err = mysql.AskQuery(ask, rows, prepare.String(), args...)
	return
}
`
	// query table.columns, assemble structure data
	for _, t := range tables {
		if t.TableName == nil {
			continue
		}
		wt := &WriteTable{}
		if t.TableComment != nil {
			wt.TableComment = *t.TableComment
		}
		flt := strings.ToLower(*t.TableName)
		wt.TableNamePascal = name.UnderlineToPascal(flt)
		wt.TableNameUnderline = name.PascalToUnderline(wt.TableNamePascal)
		var columns []*Column
		columns, err = Columns(database, *t.TableName)
		if err != nil {
			continue
		}
		// TableFirst
		assoc.WriteString(fmt.Sprintf(tmpFirst,
			wt.TableNamePascal,
			wt.TableNamePascal,
			wt.TableNamePascal,
			wt.TableNamePascal,
			DatabaseTableColumnsToScanString(columns),
			fmt.Sprintf("SELECT %s FROM `%s` WHERE (%%s) LIMIT 0, 1;", DatabaseTableColumnsToString(columns, "`"), *t.TableName),
		))
		// TablePage
		assoc.WriteString(fmt.Sprintf(tmpPage,
			wt.TableNamePascal,
			wt.TableNamePascal,
			wt.TableNamePascal,
			wt.TableNamePascal,
			DatabaseTableColumnsToScanString(columns),
			fmt.Sprintf("SELECT %s FROM `%s` WHERE (%%s)", DatabaseTableColumnsToString(columns, "`"), *t.TableName),
		))
		// TableInsert
		assoc.WriteString(fmt.Sprintf(tmpInsert,
			wt.TableNamePascal,
			wt.TableNamePascal,
			*t.TableName,
		))
		// TableDelete
		assoc.WriteString(fmt.Sprintf(tmpDelete,
			wt.TableNamePascal,
			wt.TableNamePascal,
			*t.TableName,
		))
		// TableUpdate
		assoc.WriteString(fmt.Sprintf(tmpUpdate,
			wt.TableNamePascal,
			wt.TableNamePascal,
			*t.TableName,
		))
		// TableCount
		assoc.WriteString(fmt.Sprintf(tmpCount,
			wt.TableNamePascal,
			wt.TableNamePascal,
			fmt.Sprintf("SELECT COUNT(*) AS `count` FROM `%s` WHERE (%%s);", *t.TableName),
		))
		// TableSumInt
		assoc.WriteString(fmt.Sprintf(tmpSumInt,
			wt.TableNamePascal,
			wt.TableNamePascal,
			fmt.Sprintf("SELECT SUM(%%s) AS `sum` FROM `%s` WHERE (%%s);", *t.TableName),
		))
		// TableSumFloat
		assoc.WriteString(fmt.Sprintf(tmpSumFloat,
			wt.TableNamePascal,
			wt.TableNamePascal,
			fmt.Sprintf("SELECT SUM(%%s) AS `sum` FROM `%s` WHERE (%%s);", *t.TableName),
		))
		// TableAskFirst
		assoc.WriteString(fmt.Sprintf(tmpAskFirst,
			wt.TableNamePascal,
			wt.TableNamePascal,
			wt.TableNamePascal,
			wt.TableNamePascal,
			DatabaseTableColumnsToScanString(columns),
			fmt.Sprintf("SELECT %s FROM `%s` WHERE (%%s) LIMIT 0, 1;", DatabaseTableColumnsToString(columns, "`"), *t.TableName),
		))
		// TableAskPage
		assoc.WriteString(fmt.Sprintf(tmpAskPage,
			wt.TableNamePascal,
			wt.TableNamePascal,
			wt.TableNamePascal,
			wt.TableNamePascal,
			DatabaseTableColumnsToScanString(columns),
			fmt.Sprintf("SELECT %s FROM `%s` WHERE (%%s)", DatabaseTableColumnsToString(columns, "`"), *t.TableName),
		))
		// TableAskInsert
		assoc.WriteString(fmt.Sprintf(tmpAskInsert,
			wt.TableNamePascal,
			wt.TableNamePascal,
			*t.TableName,
		))
		// TableAskDelete
		assoc.WriteString(fmt.Sprintf(tmpAskDelete,
			wt.TableNamePascal,
			wt.TableNamePascal,
			*t.TableName,
		))
		// TableAskUpdate
		assoc.WriteString(fmt.Sprintf(tmpAskUpdate,
			wt.TableNamePascal,
			wt.TableNamePascal,
			*t.TableName,
		))
		// TableAskCount
		assoc.WriteString(fmt.Sprintf(tmpAskCount,
			wt.TableNamePascal,
			wt.TableNamePascal,
			fmt.Sprintf("SELECT COUNT(*) AS `count` FROM `%s` WHERE (%%s);", *t.TableName),
		))
		// TableAskSumInt
		assoc.WriteString(fmt.Sprintf(tmpAskSumInt,
			wt.TableNamePascal,
			wt.TableNamePascal,
			fmt.Sprintf("SELECT SUM(%%s) AS `sum` FROM `%s` WHERE (%%s);", *t.TableName),
		))
		// TableAskSumFloat
		assoc.WriteString(fmt.Sprintf(tmpAskSumFloat,
			wt.TableNamePascal,
			wt.TableNamePascal,
			fmt.Sprintf("SELECT SUM(%%s) AS `sum` FROM `%s` WHERE (%%s);", *t.TableName),
		))
	}
	if filename == "" {
		filename = "query"
	}
	err = os.WriteFile(filename, assoc.Bytes(), 0644)
	if err != nil {
		return
	}
	err = FmtGoFile(filename)
	if err != nil {
		return
	}
	return
}

func WriteDatabaseToGoStructDefine(database, filename string) (err error) {
	var tables []*Table
	tables, err = Tables(database)
	if err != nil {
		return
	}
	var assoc bytes.Buffer
	// query table.columns, assemble structure data
	for _, t := range tables {
		if t.TableName == nil {
			continue
		}
		wt := &WriteTable{}
		if t.TableComment != nil {
			wt.TableComment = *t.TableComment
		}
		flt := strings.ToLower(*t.TableName)
		wt.TableNamePascal = name.UnderlineToPascal(flt)
		wt.TableNameUnderline = name.PascalToUnderline(wt.TableNamePascal)
		assoc.WriteString(fmt.Sprintf("%s %s %s\n", wt.TableNamePascal, wt.TableNameUnderline, wt.TableComment))
		var columns []*Column
		columns, err = Columns(database, *t.TableName)
		if err != nil {
			continue
		}
		for _, c := range columns {
			if c.ColumnName == nil {
				continue
			}
			wc := &WriteColumn{
				ColumnName: *c.ColumnName,
				DateType:   "",
			}
			if c.ColumnComment != nil {
				wc.ColumnComment = *c.ColumnComment
			}
			flc := strings.ToLower(wc.ColumnName)
			wc.ColumnNamePascal = name.UnderlineToPascal(flc)
			wc.ColumnNameUnderline = name.PascalToUnderline(wc.ColumnNamePascal)
			assoc.WriteString(fmt.Sprintf("\t%s %s", wc.ColumnNamePascal, ColumnToGoType(c)))
			assoc.WriteString("\n")
		}
		assoc.WriteString("\n")
	}
	if filename == "" {
		filename = "column"
	}
	err = os.WriteFile(filename, assoc.Bytes(), 0644)
	if err != nil {
		return
	}
	return
}

func WriteDatabaseToGoStructSet(database, filename string) (err error) {
	var tables []*Table
	tables, err = Tables(database)
	if err != nil {
		return
	}
	var assoc bytes.Buffer
	// query table.columns, assemble structure data
	for _, t := range tables {
		if t.TableName == nil {
			continue
		}
		wt := &WriteTable{}
		if t.TableComment != nil {
			wt.TableComment = *t.TableComment
		}
		flt := strings.ToLower(*t.TableName)
		wt.TableNamePascal = name.UnderlineToPascal(flt)
		wt.TableNameUnderline = name.PascalToUnderline(wt.TableNamePascal)
		assoc.WriteString(fmt.Sprintf("%s %s %s\n", wt.TableNamePascal, wt.TableNameUnderline, wt.TableComment))
		var columns []*Column
		columns, err = Columns(database, *t.TableName)
		if err != nil {
			continue
		}
		for _, c := range columns {
			if c.ColumnName == nil {
				continue
			}
			wc := &WriteColumn{
				ColumnName: *c.ColumnName,
				DateType:   "",
			}
			if c.ColumnComment != nil {
				wc.ColumnComment = *c.ColumnComment
			}
			flc := strings.ToLower(wc.ColumnName)
			wc.ColumnNamePascal = name.UnderlineToPascal(flc)
			wc.ColumnNameUnderline = name.PascalToUnderline(wc.ColumnNamePascal)
			gdv := ""
			if c.ColumnDefault == nil {
				gdv = "nil"
			} else {
				gdv = strings.ToLower(fmt.Sprintf("%v", *c.ColumnDefault))
			}
			if gdv == "null" {
				gdv = "nil"
			}
			if gdv == "" {
				gdv = "\"\""
			}
			if gdv == "''" {
				gdv = "\"\""
			}
			assoc.WriteString(fmt.Sprintf("\t%s: %s,\n", wc.ColumnNamePascal, gdv))
		}
		assoc.WriteString(fmt.Sprintf("\n"))
	}
	if filename == "" {
		filename = "column.map"
	}
	err = os.WriteFile(filename, assoc.Bytes(), 0644)
	if err != nil {
		return
	}
	return
}

// WriteAll write mysql structure into file
func WriteAll(host string, port int, user string, pass string, charset string, database string, director string, pkg string) (err error) {
	pool, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s",
		user,
		pass,
		host,
		port,
		database,
		charset,
	))
	if err != nil {
		return
	}
	pool.SetConnMaxLifetime(time.Minute * 3)
	pool.SetMaxOpenConns(32)
	pool.SetMaxIdleConns(32)
	defer pool.Close()

	dir := ""
	dir, err = filepath.Abs(director)
	if err != nil {
		return
	}
	_, err = os.Stat(dir)
	if err != nil {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return
		}
	}
	ps := os.PathSeparator
	pss := string(ps)
	if !strings.HasSuffix(dir, pss) {
		dir += pss
	}
	err = WriteDatabaseToGoStruct(database, dir+"mysql_struct_details.go", pkg)
	if err != nil {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return
		}
	}
	err = WriteDatabaseToGoMap(database, dir+"mysql_struct_details.map")
	if err != nil {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return
		}
	}
	err = WriteDatabaseToGoScan(database, dir+"mysql_struct_details.scan")
	if err != nil {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return
		}
	}
	err = WriteDatabaseToGoSlice(database, dir+"mysql_struct_details.slice")
	if err != nil {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return
		}
	}
	err = WriteDatabaseToGoColumn(database, dir+"mysql_struct_details.string")
	if err != nil {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return
		}
	}
	err = WriteDatabaseQuicklyQuery(database, dir+"mysql_query_details.go", pkg)
	if err != nil {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return
		}
	}
	err = WriteDatabaseToGoStructDefine(database, dir+"mysql_struct_details.struct.define")
	if err != nil {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return
		}
	}
	err = WriteDatabaseToGoStructSet(database, dir+"mysql_struct_details.struct.set")
	if err != nil {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return
		}
	}
	return
}

// export all table structures of a mysql database

package mg

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/xooooooox/gdb/mysql"
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

var (
	BD *BagDatabase
)

type MysqlTable struct {
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

func QueryTable(database string) (bag []*MysqlTable, err error) {
	fc := func(rows *sql.Rows) (err error) {
		for rows.Next() {
			s := &MysqlTable{}
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
	err = mysql.Query(fc, DqlDatabaseTables, database)
	return
}

type MysqlColumn struct {
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

func QueryColumn(database, table string) (bag []*MysqlColumn, err error) {
	fc := func(rows *sql.Rows) (err error) {
		for rows.Next() {
			s := &MysqlColumn{}
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
	err = mysql.Query(fc, DqlDatabaseTableColumns, database, table)
	return
}

func MysqlColumnToGoType(c *MysqlColumn) (ts string) {
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

func FmtGoFile(file string) error {
	cmd := exec.Command("go", "fmt", file)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

type BagDatabase struct {
	DatabaseName string
	BagTable     []*BagTable
}

type BagTable struct {
	TableName          string
	TableComment       string
	TableNamePascal    string
	TableNameUnderline string
	BagColumn          []*BagColumn
	MysqlColumn        []*MysqlColumn
	MysqlTable         *MysqlTable
}

type BagColumn struct {
	ColumnName          string
	ColumnComment       string
	ColumnNamePascal    string
	ColumnNameUnderline string
	DateType            string
	MysqlColumn         *MysqlColumn
	GoDefaultValue      string
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

func ColumnsToString(columns []*MysqlColumn, coated string) string {
	var cs []string
	for _, c := range columns {
		if c.ColumnName == nil {
			continue
		}
		cs = append(cs, fmt.Sprintf("%s%s%s", coated, *c.ColumnName, coated))
	}
	return strings.Join(cs, ", ")
}

func ColumnsToScanString(columns []*MysqlColumn) string {
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

func MethodTableName(table *BagTable) string {
	tmp := `
func (s *%s) MtTableName() string {
	return "%s"
}
`
	return fmt.Sprintf(tmp, table.TableNamePascal, table.TableName)
}

func MethodFirstById(table *BagTable) string {
	tmp := `
func (s *%s) MtFirstById(id int64) (*%s, error) {
	return %sFirst(mysql.ColumnEqual("id"), id)
}
`
	return fmt.Sprintf(tmp, table.TableNamePascal, table.TableNamePascal, table.TableNamePascal)
}

func MethodColSql(table *BagTable) string {
	tmp := `
func (s *%s) MtColSql() string {
	return "%s"
}
`
	return fmt.Sprintf(tmp, table.TableNamePascal, ColumnsToString(table.MysqlColumn, "`"))
}

func MethodColSlice(table *BagTable) string {
	tmp := `
func (s *%s) MtColSlice() []string {
	return []string{%s}
}
`
	cs := make([]string, 0, 0)
	for _, c := range table.MysqlColumn {
		if c.ColumnName == nil {
			continue
		}
		cs = append(cs, fmt.Sprintf("\"%s\"", *c.ColumnName))
	}
	return fmt.Sprintf(tmp, table.TableNamePascal, strings.Join(cs, ", "))
}

func ConstTableName(filename, pkg string) (err error) {
	var all, tmp bytes.Buffer
	all.WriteString(fmt.Sprintf("package %s\n\n", pkg))
	tmp.WriteString(fmt.Sprintf("const (\n"))
	for _, t := range BD.BagTable {
		if t.MysqlTable.TableName == nil {
			continue
		}
		tmp.WriteString(fmt.Sprintf("\tUt%s = \"%s\" // %s\n", t.TableNamePascal, t.TableNameUnderline, t.TableComment))
	}
	tmp.WriteString(")\n")
	all.Write(tmp.Bytes())
	err = os.WriteFile(filename, all.Bytes(), 0644)
	if err != nil {
		return
	}
	err = FmtGoFile(filename)
	if err != nil {
		return
	}
	return
}

func ConstSelectColumn(filename, pkg string) (err error) {
	var all, tmp bytes.Buffer
	all.WriteString(fmt.Sprintf("package %s\n\n", pkg))
	tmp.WriteString(fmt.Sprintf("const (\n"))
	for _, t := range BD.BagTable {
		if t.MysqlTable.TableName == nil {
			continue
		}
		tmp.WriteString(fmt.Sprintf("\tCs%s = \"%s\" // %s\n", t.TableNamePascal, ColumnsToString(t.MysqlColumn, "`"), t.TableComment))
	}
	tmp.WriteString(")\n")
	all.Write(tmp.Bytes())
	err = os.WriteFile(filename, all.Bytes(), 0644)
	if err != nil {
		return
	}
	err = FmtGoFile(filename)
	if err != nil {
		return
	}
	return
}

func ConstColumnName(filename, pkg string) (err error) {
	var all, tmp bytes.Buffer
	all.WriteString(fmt.Sprintf("package %s\n\n", pkg))
	tmp.WriteString(fmt.Sprintf("const (\n"))
	uniqueColumnMap := map[string]string{}
	for _, t := range BD.BagTable {
		if t.MysqlTable.TableName == nil {
			continue
		}
		for _, c := range t.BagColumn {
			uniqueColumnMap[c.ColumnNamePascal] = c.ColumnName
		}
	}
	uk, uv := AscMapStringString(uniqueColumnMap)
	length := len(uk)
	for i := 0; i < length; i++ {
		tmp.WriteString(fmt.Sprintf("\tUc%s = \"%s\"\n", uk[i], uv[i]))
	}
	tmp.WriteString(")\n")
	all.Write(tmp.Bytes())
	err = os.WriteFile(filename, all.Bytes(), 0644)
	if err != nil {
		return
	}
	err = FmtGoFile(filename)
	if err != nil {
		return
	}
	return
}

func TypeTableStruct(filename, pkg string) (err error) {
	var all bytes.Buffer
	all.WriteString(fmt.Sprintf("package %s\n\n", pkg))
	length := len(BD.BagTable)
	for i := 0; i < length; i++ {
		all.WriteString(fmt.Sprintf("// %s %s %s\n", BD.BagTable[i].TableNamePascal, BD.BagTable[i].TableNameUnderline, BD.BagTable[i].TableComment))
		all.WriteString(fmt.Sprintf("type %s struct {\n", BD.BagTable[i].TableNamePascal))
		for _, c := range BD.BagTable[i].BagColumn {
			all.WriteString(fmt.Sprintf("\t%s %s `json:\"%s\"` // %s\n", c.ColumnNamePascal, c.DateType, c.ColumnNameUnderline, c.ColumnComment))
		}
		all.WriteString("}\n")
		if i+1 != length {
			all.WriteString("\n")
		}
	}
	err = os.WriteFile(filename, all.Bytes(), 0644)
	if err != nil {
		return
	}
	err = FmtGoFile(filename)
	if err != nil {
		return
	}
	return
}

func MethodTable(filename, pkg string) (err error) {
	var all bytes.Buffer
	all.WriteString(fmt.Sprintf("package %s\n\n", pkg))
	head := `import (
	"github.com/xooooooox/gdb/mysql"
)
`
	all.WriteString(head)
	for _, t := range BD.BagTable {
		if t.MysqlTable.TableName == nil {
			continue
		}
		all.WriteString(MethodTableName(t))
		all.WriteString(MethodFirstById(t))
		all.WriteString(MethodColSql(t))
		all.WriteString(MethodColSlice(t))
	}
	err = os.WriteFile(filename, all.Bytes(), 0644)
	if err != nil {
		return
	}
	// err = FmtGoFile(filename)
	// if err != nil {
	// 	return
	// }
	return
}

var TmpFuncQueryExecute = `package %s

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/xooooooox/gdb/mysql"
)
`

var TmpFuncQueryExecuteFirst = `
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

var TmpFuncQueryExecuteInsert = `
// %sInsert execute insert one record
func %sInsert(insert map[string]interface{}) (rowsAffected int64, err error) {
	return mysql.Insert("%s", insert)
}
`

var TmpFuncQueryExecuteDelete = `
// %sDelete execute delete
func %sDelete(where string, args ...interface{}) (rowsAffected int64, err error) {
	return mysql.Delete("%s", where, args...)
}
`

var TmpFuncQueryExecuteUpdate = `
// %sUpdate execute update
func %sUpdate(update map[string]interface{}, where string, args ...interface{}) (rowsAffected int64, err error) {
	return mysql.Update("%s", update, where, args...)
}
`

var TmpFuncQueryExecuteCount = `
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

var TmpFuncQueryExecuteSumInt = `
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

var TmpFuncQueryExecuteSumFloat = `
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

var TmpFuncQueryExecuteAskFirst = `
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

var TmpFuncQueryExecuteAskPage = `
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

var TmpFuncQueryExecuteAskInsert = `
// %sAskInsert execute insert one record
func %sAskInsert(ask *sql.Tx, insert map[string]interface{}) (rowsAffected int64, err error) {
	return mysql.AskInsert(ask, "%s", insert)
}
`

var TmpFuncQueryExecuteAskDelete = `
// %sAskDelete execute delete
func %sAskDelete(ask *sql.Tx, where string, args ...interface{}) (rowsAffected int64, err error) {
	return mysql.AskDelete(ask, "%s", where, args...)
}
`

var TmpFuncQueryExecuteAskUpdate = `
// %sAskUpdate execute update
func %sAskUpdate(ask *sql.Tx, update map[string]interface{}, where string, args ...interface{}) (rowsAffected int64, err error) {
	return mysql.AskUpdate(ask, "%s", update, where, args...)
}
`

var TmpFuncQueryExecuteAskCount = `
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

var TmpFuncQueryExecuteAskSumInt = `
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

var TmpFuncQueryExecuteAskSumFloat = `
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

var TmpFuncQueryExecutePage = `
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

func FuncQueryExecute(filename, pkg string) (err error) {
	var assoc bytes.Buffer
	assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecute, pkg))

	for _, t := range BD.BagTable {
		if t.MysqlTable.TableName == nil {
			continue
		}
		// TableFirst
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteFirst,
			t.TableNamePascal,
			t.TableNamePascal,
			t.TableNamePascal,
			t.TableNamePascal,
			ColumnsToScanString(t.MysqlColumn),
			fmt.Sprintf("SELECT %s FROM `%s` WHERE (%%s) LIMIT 0, 1;", ColumnsToString(t.MysqlColumn, "`"), t.TableName),
		))
		// TablePage
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecutePage,
			t.TableNamePascal,
			t.TableNamePascal,
			t.TableNamePascal,
			t.TableNamePascal,
			ColumnsToScanString(t.MysqlColumn),
			fmt.Sprintf("SELECT %s FROM `%s` WHERE (%%s)", ColumnsToString(t.MysqlColumn, "`"), t.TableName),
		))
		// TableInsert
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteInsert,
			t.TableNamePascal,
			t.TableNamePascal,
			t.TableName,
		))
		// TableDelete
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteDelete,
			t.TableNamePascal,
			t.TableNamePascal,
			t.TableName,
		))
		// TableUpdate
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteUpdate,
			t.TableNamePascal,
			t.TableNamePascal,
			t.TableName,
		))
		// TableCount
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteCount,
			t.TableNamePascal,
			t.TableNamePascal,
			fmt.Sprintf("SELECT COUNT(*) AS `count` FROM `%s` WHERE (%%s);", t.TableName),
		))
		// TableSumInt
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteSumInt,
			t.TableNamePascal,
			t.TableNamePascal,
			fmt.Sprintf("SELECT SUM(%%s) AS `sum` FROM `%s` WHERE (%%s);", t.TableName),
		))
		// TableSumFloat
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteSumFloat,
			t.TableNamePascal,
			t.TableNamePascal,
			fmt.Sprintf("SELECT SUM(%%s) AS `sum` FROM `%s` WHERE (%%s);", t.TableName),
		))
		// TableAskFirst
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteAskFirst,
			t.TableNamePascal,
			t.TableNamePascal,
			t.TableNamePascal,
			t.TableNamePascal,
			ColumnsToScanString(t.MysqlColumn),
			fmt.Sprintf("SELECT %s FROM `%s` WHERE (%%s) LIMIT 0, 1;", ColumnsToString(t.MysqlColumn, "`"), t.TableName),
		))
		// TableAskPage
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteAskPage,
			t.TableNamePascal,
			t.TableNamePascal,
			t.TableNamePascal,
			t.TableNamePascal,
			ColumnsToScanString(t.MysqlColumn),
			fmt.Sprintf("SELECT %s FROM `%s` WHERE (%%s)", ColumnsToString(t.MysqlColumn, "`"), t.TableName),
		))
		// TableAskInsert
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteAskInsert,
			t.TableNamePascal,
			t.TableNamePascal,
			t.TableName,
		))
		// TableAskDelete
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteAskDelete,
			t.TableNamePascal,
			t.TableNamePascal,
			t.TableName,
		))
		// TableAskUpdate
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteAskUpdate,
			t.TableNamePascal,
			t.TableNamePascal,
			t.TableName,
		))
		// TableAskCount
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteAskCount,
			t.TableNamePascal,
			t.TableNamePascal,
			fmt.Sprintf("SELECT COUNT(*) AS `count` FROM `%s` WHERE (%%s);", t.TableName),
		))
		// TableAskSumInt
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteAskSumInt,
			t.TableNamePascal,
			t.TableNamePascal,
			fmt.Sprintf("SELECT SUM(%%s) AS `sum` FROM `%s` WHERE (%%s);", t.TableName),
		))
		// TableAskSumFloat
		assoc.WriteString(fmt.Sprintf(TmpFuncQueryExecuteAskSumFloat,
			t.TableNamePascal,
			t.TableNamePascal,
			fmt.Sprintf("SELECT SUM(%%s) AS `sum` FROM `%s` WHERE (%%s);", t.TableName),
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

func AssocMap(filename string) (err error) {
	var assoc bytes.Buffer
	length := len(BD.BagTable)
	for i := 0; i < length; i++ {
		if BD.BagTable[i].MysqlTable.TableName == nil {
			continue
		}
		assoc.WriteString(fmt.Sprintf("%s %s %s\n", BD.BagTable[i].TableNamePascal, BD.BagTable[i].TableNameUnderline, BD.BagTable[i].TableComment))
		assoc.WriteString(fmt.Sprintf("map[string]interface{}{\n"))
		for _, c := range BD.BagTable[i].BagColumn {
			assoc.WriteString(fmt.Sprintf("\t\"%s\": %s,\n", c.ColumnName, c.GoDefaultValue))
		}
		assoc.WriteString(fmt.Sprintf("}\n"))
		if i+1 != length {
			assoc.WriteString("\n")
		}
	}
	err = os.WriteFile(filename, assoc.Bytes(), 0644)
	if err != nil {
		return
	}
	return
}

func AssocScan(filename string) (err error) {
	var assoc bytes.Buffer
	length := len(BD.BagTable)
	for i := 0; i < length; i++ {
		if BD.BagTable[i].MysqlTable.TableName == nil {
			continue
		}
		assoc.WriteString(fmt.Sprintf("%s %s %s\n", BD.BagTable[i].TableNamePascal, BD.BagTable[i].TableNameUnderline, BD.BagTable[i].TableComment))
		for _, c := range BD.BagTable[i].BagColumn {
			assoc.WriteString(fmt.Sprintf("\t&s.%s,\n", c.ColumnNamePascal))
		}
		if i+1 != length {
			assoc.WriteString("\n")
		}
	}
	err = os.WriteFile(filename, assoc.Bytes(), 0644)
	if err != nil {
		return
	}
	return
}

func AssocSlice(filename string) (err error) {
	var assoc bytes.Buffer
	length := len(BD.BagTable)
	for i := 0; i < length; i++ {
		if BD.BagTable[i].MysqlTable.TableName == nil {
			continue
		}
		assoc.WriteString(fmt.Sprintf("%s %s %s\n", BD.BagTable[i].TableNamePascal, BD.BagTable[i].TableNameUnderline, BD.BagTable[i].TableComment))
		for _, c := range BD.BagTable[i].BagColumn {
			assoc.WriteString(fmt.Sprintf("\t\"%s\",\n", c.ColumnName))
		}
		if i+1 != length {
			assoc.WriteString("\n")
		}
	}
	err = os.WriteFile(filename, assoc.Bytes(), 0644)
	if err != nil {
		return
	}
	return
}

func AssocString(filename string) (err error) {
	var assoc bytes.Buffer
	length := len(BD.BagTable)
	for i := 0; i < length; i++ {
		if BD.BagTable[i].MysqlTable.TableName == nil {
			continue
		}
		assoc.WriteString(fmt.Sprintf("%s %s %s\n", BD.BagTable[i].TableNamePascal, BD.BagTable[i].TableNameUnderline, BD.BagTable[i].TableComment))
		assoc.WriteString(ColumnsToString(BD.BagTable[i].MysqlColumn, ""))
		assoc.WriteString("\n")
		assoc.WriteString(ColumnsToString(BD.BagTable[i].MysqlColumn, "`"))
		assoc.WriteString("\n")
		assoc.WriteString(ColumnsToString(BD.BagTable[i].MysqlColumn, "'"))
		assoc.WriteString("\n")
		assoc.WriteString(ColumnsToString(BD.BagTable[i].MysqlColumn, "\""))
		assoc.WriteString("\n")
		if i+1 != length {
			assoc.WriteString("\n")
		}
	}
	err = os.WriteFile(filename, assoc.Bytes(), 0644)
	if err != nil {
		return
	}
	return
}

func TypeStruct(filename string) (err error) {
	var assoc bytes.Buffer
	length := len(BD.BagTable)
	for i := 0; i < length; i++ {
		if BD.BagTable[i].MysqlTable.TableName == nil {
			continue
		}
		assoc.WriteString(fmt.Sprintf("%s %s %s\n", BD.BagTable[i].TableNamePascal, BD.BagTable[i].TableNameUnderline, BD.BagTable[i].TableComment))
		for _, c := range BD.BagTable[i].BagColumn {
			assoc.WriteString(fmt.Sprintf("\t%s %s\n", c.ColumnNamePascal, MysqlColumnToGoType(c.MysqlColumn)))
		}
		if i+1 != length {
			assoc.WriteString("\n")
		}
	}
	err = os.WriteFile(filename, assoc.Bytes(), 0644)
	if err != nil {
		return
	}
	return
}

func StructSet(filename string) (err error) {
	var assoc bytes.Buffer
	length := len(BD.BagTable)
	for i := 0; i < length; i++ {
		if BD.BagTable[i].MysqlTable.TableName == nil {
			continue
		}
		assoc.WriteString(fmt.Sprintf("%s %s %s\n", BD.BagTable[i].TableNamePascal, BD.BagTable[i].TableNameUnderline, BD.BagTable[i].TableComment))
		for _, c := range BD.BagTable[i].BagColumn {
			assoc.WriteString(fmt.Sprintf("\t\"%s\": %s,\n", c.ColumnNamePascal, c.GoDefaultValue))
		}
		if i+1 != length {
			assoc.WriteString("\n")
		}
	}
	err = os.WriteFile(filename, assoc.Bytes(), 0644)
	if err != nil {
		return
	}
	return
}

func Open(host string, port int, user string, pass string, charset string, database string) (err error) {
	var db *sql.DB
	db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s",
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
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(32)
	mysql.PutPool(db)
	return
}

func MkdirAll(director string) (dir string, err error) {
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
	return
}

func SetBD(database string) (err error) {
	BD = &BagDatabase{
		DatabaseName: database,
	}
	var tables []*MysqlTable
	tables, err = QueryTable(database)
	if err != nil {
		return
	}
	for _, tab := range tables {
		if tab.TableName == nil {
			continue
		}
		wt := &BagTable{
			TableName:  *tab.TableName,
			MysqlTable: tab,
		}
		if tab.TableComment != nil {
			wt.TableComment = *tab.TableComment
		}
		wt.TableNamePascal = name.UnderlineToPascal(strings.ToLower(*tab.TableName))
		wt.TableNameUnderline = name.PascalToUnderline(wt.TableNamePascal)
		var columns []*MysqlColumn
		columns, err = QueryColumn(database, *tab.TableName)
		if err != nil {
			continue
		}
		wt.MysqlColumn = columns
		for _, col := range columns {
			if col.ColumnName == nil {
				continue
			}
			wc := &BagColumn{
				ColumnName:  *col.ColumnName,
				DateType:    "",
				MysqlColumn: col,
			}
			if col.ColumnComment != nil {
				wc.ColumnComment = *col.ColumnComment
			}
			wc.ColumnNamePascal = name.UnderlineToPascal(strings.ToLower(wc.ColumnName))
			wc.ColumnNameUnderline = name.PascalToUnderline(wc.ColumnNamePascal)
			wc.DateType = MysqlColumnToGoType(col)
			if col.ColumnDefault == nil {
				wc.GoDefaultValue = "nil"
			} else {
				wc.GoDefaultValue = strings.ToLower(fmt.Sprintf("%v", *col.ColumnDefault))
			}
			if wc.GoDefaultValue == "null" {
				wc.GoDefaultValue = "nil"
			}
			if wc.GoDefaultValue == "" {
				wc.GoDefaultValue = "\"\""
			}
			if wc.GoDefaultValue == "''" {
				wc.GoDefaultValue = "\"\""
			}
			wt.BagColumn = append(wt.BagColumn, wc)
		}
		BD.BagTable = append(BD.BagTable, wt)
	}
	return
}

func WriteAll(host string, port int, user string, pass string, charset string, database string, director string, pkg string) (err error) {
	if err = Open(host, port, user, pass, charset, database); err != nil {
		return
	}
	defer mysql.GetPool().Close()
	dir := ""
	dir, err = MkdirAll(director)
	if err != nil {
		return
	}
	err = SetBD(database)
	if err != nil {
		return
	}
	err = ConstTableName(dir+"const_table_name.go", pkg)
	if err != nil {
		return
	}
	err = ConstSelectColumn(dir+"const_select_column.go", pkg)
	if err != nil {
		return
	}
	err = ConstColumnName(dir+"const_column_name.go", pkg)
	if err != nil {
		return
	}
	err = TypeTableStruct(dir+"type_table_struct.go", pkg)
	if err != nil {
		return
	}
	err = FuncQueryExecute(dir+"func_query_execute.go", pkg)
	if err != nil {
		return
	}
	err = MethodTable(dir+"method_table.go", pkg)
	if err != nil {
		return
	}
	err = AssocMap(dir + "assoc.map")
	if err != nil {
		return
	}
	err = AssocScan(dir + "assoc.scan")
	if err != nil {
		return
	}
	err = AssocSlice(dir + "assoc.slice")
	if err != nil {
		return
	}
	err = AssocString(dir + "assoc.string")
	if err != nil {
		return
	}
	err = TypeStruct(dir + "type.struct")
	if err != nil {
		return
	}
	err = StructSet(dir + "struct.set")
	if err != nil {
		return
	}
	return
}

// export all table structures of a mysql database

package mg

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

	_ "github.com/go-sql-driver/mysql"
	"github.com/xooooooox/gdb/mysql"

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

func MysqlColumnToGoDefaultValue(c *MysqlColumn) (val string) {
	val = "\"\""
	if c.ColumnDefault == nil {
		val = "nil"
		// primary key
		if c.ColumnKey != nil && strings.ToLower(*c.ColumnKey) == "pri" {
			// int
			if c.ColumnType != nil && strings.Index(strings.ToLower(*c.ColumnType), "int") >= 0 {
				val = "0"
			} else {
				// default string
				val = "\"\""
			}
		}
		return
	}
	val = strings.ToLower(fmt.Sprintf("%v", *c.ColumnDefault))
	if strings.ToLower(val) == "null" {
		val = "nil"
		return
	}
	if val == "" || val == "''" {
		val = "\"\""
		return
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

func ConstTableName(filename, pkg string) (err error) {
	var all, tmp bytes.Buffer
	all.WriteString(fmt.Sprintf("package %s\n\n", pkg))
	tmp.WriteString(fmt.Sprintf("const (\n"))
	for _, t := range BD.BagTable {
		if t.MysqlTable.TableName == nil {
			continue
		}
		tmp.WriteString(fmt.Sprintf("\tNt%s = \"%s\" // %s\n", t.TableNamePascal, t.TableNameUnderline, t.TableComment))
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

func VarTableColumnSlice(filename, pkg string) (err error) {
	var all, tmp bytes.Buffer
	all.WriteString(fmt.Sprintf("package %s\n\n", pkg))
	tmp.WriteString(fmt.Sprintf("var (\n"))
	for _, t := range BD.BagTable {
		if t.MysqlTable.TableName == nil {
			continue
		}
		cs := make([]string, 0, 0)
		for _, c := range t.MysqlColumn {
			if c.ColumnName == nil {
				continue
			}
			cs = append(cs, fmt.Sprintf("\"%s\"", *c.ColumnName))
		}
		tmp.WriteString(fmt.Sprintf("\t%sCol = []string{%s} // %s\n", t.TableNamePascal, strings.Join(cs, ", "), t.TableComment))
		// tmp.WriteString(fmt.Sprintf("\t%sCol []string = []string{%s} // %s\n", t.TableNamePascal, strings.Join(cs, ", "), t.TableComment))

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

func ConstTableColumnSql(filename, pkg string) (err error) {
	var all, tmp bytes.Buffer
	all.WriteString(fmt.Sprintf("package %s\n\n", pkg))
	tmp.WriteString(fmt.Sprintf("const (\n"))
	for _, t := range BD.BagTable {
		if t.MysqlTable.TableName == nil {
			continue
		}
		cs := make([]string, 0, 0)
		for _, c := range t.MysqlColumn {
			if c.ColumnName == nil {
				continue
			}
			cs = append(cs, fmt.Sprintf("\"%s\"", *c.ColumnName))
		}
		tmp.WriteString(fmt.Sprintf("\t%sColSql = \"%s\" // %s\n", t.TableNamePascal, ColumnsToString(t.MysqlColumn, "`"), t.TableComment))
		// tmp.WriteString(fmt.Sprintf("\t%sColSql string = \"%s\" // %s\n", t.TableNamePascal, ColumnsToString(t.MysqlColumn,"`"), t.TableComment))

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
		tmp.WriteString(fmt.Sprintf("\tNc%s = \"%s\"\n", uk[i], uv[i]))
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

func SelectAllColumnFromTable(cs []*MysqlColumn, table string) string {
	return fmt.Sprintf("SELECT %s FROM `%s`", ColumnsToString(cs, "`"), table)
}

func FindColumnPrimaryKeyName(cs []*MysqlColumn) string {
	for _, c := range cs {
		if c.ColumnKey == nil {
			continue
		}
		if strings.Index(strings.ToLower(*c.ColumnKey), "pri") < 0 {
			continue
		}
		if c.ColumnName == nil {
			continue
		}
		return *c.ColumnName
	}
	return "id"
}

func VarScanFuncTable(filename, pkg string) (err error) {
	var tmpVarScan = `package %s

	import (
		"database/sql"
	)
	`
	var tmpScanOne = `func ScanOne%s(rows *sql.Rows) (s *%s, err error) {
		if rows.Next() {
			s = &%s{}
			err = rows.Scan(
				%s
			)
			if err != nil {
				return
			}
		}
		return
	}`
	var tmpScanAll = `func ScanAll%s(rows *sql.Rows) (ss []*%s, err error) {
		for rows.Next() {
			s := &%s{}
			err = rows.Scan(
				%s
			)
			if err != nil {
				return
			}
			ss = append(ss, s)
		}
		return
	}`
	var assoc bytes.Buffer
	assoc.WriteString(fmt.Sprintf(tmpVarScan, pkg))

	for _, t := range BD.BagTable {
		if t.MysqlTable.TableName == nil {
			continue
		}
		assoc.WriteString("\n")
		assoc.WriteString(fmt.Sprintf(tmpScanOne, t.TableNamePascal, t.TableNamePascal, t.TableNamePascal, ColumnsToScanString(t.MysqlColumn)))
		assoc.WriteString("\n\n")
		assoc.WriteString(fmt.Sprintf(tmpScanAll, t.TableNamePascal, t.TableNamePascal, t.TableNamePascal, ColumnsToScanString(t.MysqlColumn)))
		assoc.WriteString("\n")
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
			assoc.WriteString(fmt.Sprintf("\t%s: %s,\n", c.ColumnNamePascal, c.GoDefaultValue))
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
			wc.GoDefaultValue = MysqlColumnToGoDefaultValue(col)
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
	err = ConstTableName(dir+"mg1_const_table_name.go", pkg)
	if err != nil {
		return
	}
	err = VarTableColumnSlice(dir+"mg1_var_table_column_slice.go", pkg)
	if err != nil {
		return
	}
	err = ConstTableColumnSql(dir+"mg1_const_table_column_sql.go", pkg)
	if err != nil {
		return
	}
	err = ConstColumnName(dir+"mg1_const_column_name.go", pkg)
	if err != nil {
		return
	}
	err = TypeTableStruct(dir+"mg1_type_table_struct.go", pkg)
	if err != nil {
		return
	}
	err = VarScanFuncTable(dir+"mg1_var_table_scan.go", pkg)
	if err != nil {
		return
	}
	err = AssocMap(dir + "mg2_assoc.map")
	if err != nil {
		return
	}
	err = AssocScan(dir + "mg2_assoc.scan")
	if err != nil {
		return
	}
	err = AssocSlice(dir + "mg2_assoc.slice")
	if err != nil {
		return
	}
	err = AssocString(dir + "mg2_assoc.string")
	if err != nil {
		return
	}
	err = TypeStruct(dir + "mg2_type.struct")
	if err != nil {
		return
	}
	err = StructSet(dir + "mg2_struct.set")
	if err != nil {
		return
	}
	return
}

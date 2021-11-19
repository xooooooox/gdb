package main

import (
	"bytes"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

var username = flag.String("u", "root", "mysql username")

var password = flag.String("p", "", "mysql password")

var host = flag.String("h", "127.0.0.1", "mysql host")

var port = flag.Int("P", 3306, "mysql port")

var database = flag.String("d", "", "mysql database")

var charset = flag.String("c", "utf8mb4", "mysql charset")

var collation = flag.String("l", "utf8mb4_unicode_ci", "mysql collation")

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}
	err := Write(*username, *password, *host, *port, *database, *charset, *collation)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
	}
}

func Write(username, password, host string, port int, dbname, charset, collation string) (err error) {
	if con := Db1(); con == nil {
		err = Open(fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&collation=%s", username, password, host, port, dbname, charset, collation))
		if err != nil {
			return
		}
	}
	dbs := NewDatabase(dbname)
	// 查询数据库的所有表
	err = dbs.QueryTable()
	if err != nil {
		return
	}
	// 查询表的所有字段
	for _, t := range dbs.Table {
		err = t.QueryColumn(dbs.Name)
		if err != nil {
			return
		}
	}
	var fer bytes.Buffer
	var bts []byte
	fer.WriteString(`package main

import (
	"database/sql"
)
`)
	bts, err = dbs.TemplateSql()
	if err != nil {
		return
	} else {
		fer.Write(bts)
	}
	bts, err = dbs.TemplateScan()
	if err != nil {
		return
	} else {
		fer.Write(bts)
	}
	bts, err = dbs.GoTypeTableStruct()
	if err != nil {
		return
	} else {
		fer.Write(bts)
	}
	var fil *os.File
	fil, err = os.Create("db.go")
	if err != nil {
		return
	}
	defer fil.Close()
	_, err = fil.Write(fer.Bytes())
	if err != nil {
		return
	}
	err = FmtGoFile("db.go")
	if err != nil {
		return
	}
	return
}

// =====================================================================================================================
// custom function
// =====================================================================================================================

// PascalToUnderline XxxYyy to xxx_yyy
func PascalToUnderline(s string) string {
	var tmp []byte
	j := false
	num := len(s)
	for i := 0; i < num; i++ {
		d := s[i]
		if i > 0 && d >= 'A' && d <= 'Z' && j {
			tmp = append(tmp, '_')
		}
		if d != '_' {
			j = true
		}
		tmp = append(tmp, d)
	}
	return strings.ToLower(string(tmp[:]))
}

// UnderlineToPascal xxx_yyy to XxxYyy
func UnderlineToPascal(s string) string {
	var tmp []byte
	bts := []byte(s)
	length := len(bts)
	nextLetterNeedToUpper := true
	for i := 0; i < length; i++ {
		if bts[i] == '_' {
			nextLetterNeedToUpper = true
			continue
		}
		if nextLetterNeedToUpper && bts[i] >= 'a' && bts[i] <= 'z' {
			tmp = append(tmp, bts[i]-32)
		} else {
			tmp = append(tmp, bts[i])
		}
		nextLetterNeedToUpper = false
	}
	return string(tmp[:])
}

func FmtGoFile(file string) error {
	cmd := exec.Command("go", "fmt", file)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
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

// =====================================================================================================================
// write mysql database structure to go source file
// =====================================================================================================================

type Database struct {
	Name  string   // 数据库名
	Table []*Table // 数据库中的所有表
}

func NewDatabase(name string) *Database {
	return &Database{
		Name: name,
	}
}

type Table struct {
	TableSchema    *string   // 数据库名
	TableName      *string   // 表名
	TableType      *string   // 表类型
	Engine         *string   // 表存储引擎
	RowFormat      *string   // 行格式
	TableRows      *int      // 表已经存储数据的行数
	AutoIncrement  *int      // 自动递增值
	CreateTime     *string   // 创建时间
	UpdateTime     *string   // 更新时间
	TableCollation *string   // 校对集名称
	TableComment   *string   // 表注释
	Column         []*Column // 表中的所有字段
}

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

func (a *Database) QueryTable() (err error) {
	fc := func(rows *sql.Rows) (err error) {
		if a.Table == nil {
			a.Table = []*Table{}
		}
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
			a.Table = append(a.Table, s)
		}
		return
	}
	err = Query(fc, "SELECT `TABLE_SCHEMA`, `TABLE_NAME`, `TABLE_TYPE`, `ENGINE`, `ROW_FORMAT`, `TABLE_ROWS`, `AUTO_INCREMENT`, `CREATE_TIME`, `UPDATE_TIME`, `TABLE_COLLATION`, `TABLE_COMMENT` FROM `information_schema`.`TABLES` WHERE ( `TABLE_SCHEMA` = ? AND `TABLE_TYPE` = 'BASE TABLE' ) ORDER BY `TABLE_NAME` ASC;", a.Name)
	return
}

func (a *Database) GoTypeTableStruct() (bts []byte, err error) {
	var tmp bytes.Buffer
	tmp.WriteString("\n")
	length := len(a.Table)
	for i := 0; i < length; i++ {
		tmp.WriteString(fmt.Sprintf("// %s %s %s\n", UnderlineToPascal(*(a.Table[i].TableName)), *(a.Table[i].TableName), *(a.Table[i].TableComment)))
		tmp.WriteString(fmt.Sprintf("type %s struct {\n", UnderlineToPascal(*(a.Table[i].TableName))))
		for _, c := range a.Table[i].Column {
			tmp.WriteString(fmt.Sprintf("\t%s %s `json:\"%s\"` // %s\n", UnderlineToPascal(*c.ColumnName), c.ColumnTypeToGoType(), *c.ColumnName, *c.ColumnComment))
		}
		tmp.WriteString("}\n")
		if i+1 != length {
			tmp.WriteString("\n")
		}
	}
	bts = tmp.Bytes()
	return
}

func (a *Database) TemplateSql() (bts []byte, err error) {
	var tmp bytes.Buffer
	var pri string
	var colsWithoutPriStr string
	var colsWithoutPri []string
	var cols []string
	var colsStr string
	var names string
	tmp.WriteString("\nconst (\n")
	for _, t := range a.Table {
		names = UnderlineToPascal(*t.TableName)
		colsWithoutPri = t.ColumnSqlStringWithoutPrimaryKey("`")
		cols = make([]string, len(colsWithoutPri))
		for index := range colsWithoutPri {
			cols[index] = "?"
		}
		colsWithoutPriStr = strings.Join(colsWithoutPri, ", ")
		colsStr = strings.Join(cols, ", ")
		pri = t.FindColumnPrimaryKeyName()
		tmp.WriteString(fmt.Sprintf("\t%sInsertSql = \"INSERT INTO `%s` ( %s ) VALUES ( %s );\"\n", names, *t.TableName, colsWithoutPriStr, colsStr))
		tmp.WriteString(fmt.Sprintf("\t%sDeleteSql = \"DELETE FROM `%s` WHERE ( `%s` = ? );\"\n", names, *t.TableName, pri))
		tmp.WriteString(fmt.Sprintf("\t%sUpdateSql = \"UPDATE `%s` SET %s WHERE ( `%s` = ? );\"\n", names, *t.TableName, t.ColumnSetSqlStringWithoutPrimaryKey("`"), pri))
		tmp.WriteString(fmt.Sprintf("\t%sSelectSql = \"SELECT %s FROM `%s` WHERE ( `%s` = ? ) ORDER BY `%s` DESC LIMIT 0, 1;\"\n", names, t.ColumnSqlString("`"), *t.TableName, pri, pri))
	}
	tmp.WriteString(")\n")
	bts = tmp.Bytes()
	return
}

func (a *Database) TemplateScan() (bts []byte, err error) {
	var tmp bytes.Buffer
	var scan string
	var names string
	tmp.WriteString("\n")
	one := `
func %sScan(rows *sql.Rows) (s *%s, err error) {
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
}%s`
	all := `
func %sScanAll(rows *sql.Rows) (ss []*%s, err error) {
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
}%s`
	for _, t := range a.Table {
		names = UnderlineToPascal(*t.TableName)
		scan = t.ColumnToScanString()
		tmp.WriteString(fmt.Sprintf(one, names, names, names, scan, "\n"))
		tmp.WriteString(fmt.Sprintf(all, names, names, names, scan, "\n"))
	}
	bts = tmp.Bytes()
	return
}

func (a *Table) QueryColumn(database string) (err error) {
	fc := func(rows *sql.Rows) (err error) {
		if a.Column == nil {
			a.Column = []*Column{}
		}
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
			a.Column = append(a.Column, s)
		}
		return
	}
	err = Query(fc, "SELECT `TABLE_SCHEMA`, `TABLE_NAME`, `COLUMN_NAME`, `ORDINAL_POSITION`, `COLUMN_DEFAULT`, `IS_NULLABLE`, `DATA_TYPE`, `CHARACTER_MAXIMUM_LENGTH`, `CHARACTER_OCTET_LENGTH`, `NUMERIC_PRECISION`, `NUMERIC_SCALE`, `CHARACTER_SET_NAME`, `COLLATION_NAME`, `COLUMN_TYPE`, `COLUMN_KEY`, `EXTRA`, `COLUMN_COMMENT` FROM `information_schema`.`COLUMNS` WHERE ( `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ? ) ORDER BY `ORDINAL_POSITION` ASC;", database, a.TableName)
	return
}

func (a *Table) ColumnSqlString(coated string) string {
	var cs []string
	for _, c := range a.Column {
		if c.ColumnName == nil {
			continue
		}
		cs = append(cs, fmt.Sprintf("%s%s%s", coated, *c.ColumnName, coated))
	}
	return strings.Join(cs, ", ")
}

func (a *Table) ColumnSqlStringWithoutPrimaryKey(coated string) (cs []string) {
	pri := a.FindColumnPrimaryKeyName()
	for _, c := range a.Column {
		if c.ColumnName == nil {
			continue
		}
		if pri != *c.ColumnName {
			cs = append(cs, fmt.Sprintf("%s%s%s", coated, *c.ColumnName, coated))
		}
	}
	return
}

func (a *Table) ColumnSetSqlString(coated string) string {
	var cs []string
	for _, c := range a.Column {
		if c.ColumnName == nil {
			continue
		}
		cs = append(cs, fmt.Sprintf("%s%s%s = ?", coated, *c.ColumnName, coated))
	}
	return strings.Join(cs, ", ")
}

func (a *Table) ColumnSetSqlStringWithoutPrimaryKey(coated string) string {
	var cs []string
	pri := a.FindColumnPrimaryKeyName()
	for _, c := range a.Column {
		if c.ColumnName == nil {
			continue
		}
		if pri != *c.ColumnName {
			cs = append(cs, fmt.Sprintf("%s%s%s = ?", coated, *c.ColumnName, coated))
		}
	}
	return strings.Join(cs, ", ")
}

func (a *Table) FindColumnPrimaryKeyName() string {
	for _, c := range a.Column {
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

func (a *Table) ColumnToScanString() string {
	var cs bytes.Buffer
	length := len(a.Column)
	for i := 0; i < length; i++ {
		if a.Column[i].ColumnName == nil {
			continue
		}
		cs.WriteString(fmt.Sprintf("\t\t\t&s.%s,", UnderlineToPascal(strings.ToLower(*(a.Column[i].ColumnName)))))
		if i < length-1 {
			cs.WriteString("\n")
		}
	}
	return cs.String()
}

func (c *Column) ColumnTypeToGoType() (types string) {
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
				types = "uint8"
			} else if strings.Index(datatype, "smallint") >= 0 {
				types = "uint16"
			} else if strings.Index(datatype, "mediumint") >= 0 || strings.Index(datatype, "integer") >= 0 {
				types = "uint"
			} else if strings.Index(datatype, "bigint") >= 0 {
				types = "uint64"
			} else {
				types = "uint"
			}
		} else {
			if strings.Index(datatype, "tinyint") >= 0 {
				types = "int8"
			} else if strings.Index(datatype, "smallint") >= 0 {
				types = "int16"
			} else if strings.Index(datatype, "mediumint") >= 0 || strings.Index(datatype, "integer") >= 0 {
				types = "int"
			} else if strings.Index(datatype, "bigint") >= 0 {
				types = "int64"
			} else {
				types = "int"
			}
		}
	} else if strings.Index(datatype, "float") >= 0 || strings.Index(datatype, "double") >= 0 || strings.Index(datatype, "decimal") >= 0 || strings.Index(datatype, "numeric") >= 0 {
		// 近似值 float double; 精确值 decimal,numeric
		types = "float64"
	} else if datatype == "varchar" {
		types = "string"
	} else if datatype == "char" {
		types = "string"
	} else if strings.Index(datatype, "binary") >= 0 || strings.Index(datatype, "varbinary") >= 0 {
		types = "[]byte"
	} else if strings.Index(datatype, "text") >= 0 {
		types = "string"
	} else if strings.Index(datatype, "blob") >= 0 {
		types = "[]byte"
	} else if strings.Index(datatype, "enum") >= 0 || strings.Index(datatype, "set") >= 0 {
		types = "string"
	} else {
		types = "string"
	}
	if nullable && types != "" && strings.Index(types, "byte") < 0 {
		types = "*" + types
	}
	return
}

func (c *Column) ColumnTypeToSetGoDefaultValue() (val string) {
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

// =====================================================================================================================
// database mysql
// =====================================================================================================================

// db database object
var db *sql.DB

// ErrorTransactionNotOpened transaction not opened
var ErrorTransactionNotOpened = errors.New("mysql: please open the transaction first")

// Open connect to mysql service, auto set database connect; dsn: runner:112233@tcp(127.0.0.1:3306)/running?charset=utf8mb4&collation=utf8mb4_unicode_ci
func Open(dsn string) (err error) {
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(512)
	db.SetMaxIdleConns(128)
	return
}

func Db0(database *sql.DB) {
	db = database
}

func Db1() *sql.DB {
	return db
}

func Exec() *Execs {
	return &Execs{
		db: db,
	}
}

func Query(anonymous func(rows *sql.Rows) (err error), prepare string, args ...interface{}) error {
	return Exec().OneStepQuery(anonymous, prepare, args...)
}

func Execute(prepare string, args ...interface{}) (int64, error) {
	return Exec().OneStepExecute(prepare, args...)
}

func AddOne(prepare string, args ...interface{}) (int64, error) {
	return Exec().OneStepAddOne(prepare, args...)
}

func Transaction(times int, anonymous func(execs *Execs) (err error)) error {
	return Exec().Transaction(times, anonymous)
}

// Execs mysql database sql statement execute object
type Execs struct {
	db      *sql.DB                          // database connection object
	tx      *sql.Tx                          // database transaction object
	prepare string                           // sql statement to be executed
	args    []interface{}                    // executed sql parameters
	scan    func(rows *sql.Rows) (err error) // scan query results
}

func (s *Execs) Begin() (err error) {
	s.tx, err = s.db.Begin()
	return
}

func (s *Execs) Rollback() (err error) {
	if s.tx == nil {
		err = ErrorTransactionNotOpened
		return
	}
	err = s.tx.Rollback()
	s.tx = nil
	return
}

func (s *Execs) Commit() (err error) {
	if s.tx == nil {
		err = ErrorTransactionNotOpened
		return
	}
	err = s.tx.Commit()
	s.tx = nil
	return
}

func (s *Execs) Scan(anonymous func(rows *sql.Rows) (err error)) *Execs {
	s.scan = anonymous
	return s
}

func (s *Execs) Prepare(prepare string) *Execs {
	s.prepare = prepare
	return s
}

func (s *Execs) Args(args ...interface{}) *Execs {
	s.args = args
	return s
}

func (s *Execs) Stmt() (stmt *sql.Stmt, err error) {
	if s.tx != nil {
		stmt, err = s.tx.Prepare(s.prepare)
	} else {
		stmt, err = s.db.Prepare(s.prepare)
	}
	return
}

func (s *Execs) FetchSql() (prepare string, args []interface{}) {
	prepare, args = s.prepare, s.args
	return
}

func (s *Execs) Query() (err error) {
	var stmt *sql.Stmt
	stmt, err = s.Stmt()
	if err != nil {
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.Query(s.args...)
	if err != nil {
		return
	}
	defer rows.Close()
	err = s.scan(rows)
	return
}

func (s *Execs) Execute() (rowsAffected int64, err error) {
	var stmt *sql.Stmt
	stmt, err = s.Stmt()
	if err != nil {
		return
	}
	defer stmt.Close()
	var result sql.Result
	result, err = stmt.Exec(s.args...)
	if err != nil {
		return
	}
	rowsAffected, err = result.RowsAffected()
	return
}

func (s *Execs) AddOne() (lastId int64, err error) {
	var stmt *sql.Stmt
	stmt, err = s.Stmt()
	if err != nil {
		return
	}
	defer stmt.Close()
	var result sql.Result
	result, err = stmt.Exec(s.args...)
	if err != nil {
		return
	}
	lastId, err = result.LastInsertId()
	return
}

func (s *Execs) OneStepQuery(anonymous func(rows *sql.Rows) (err error), prepare string, args ...interface{}) (err error) {
	err = s.Scan(anonymous).Prepare(prepare).Args(args...).Query()
	return
}

func (s *Execs) OneStepExecute(prepare string, args ...interface{}) (int64, error) {
	return s.Prepare(prepare).Args(args...).Execute()
}

func (s *Execs) OneStepAddOne(prepare string, args ...interface{}) (int64, error) {
	return s.Prepare(prepare).Args(args...).AddOne()
}

// Transaction closure execute transaction, automatic rollback on error
func (s *Execs) Transaction(times int, anonymous func(execs *Execs) (err error)) (err error) {
	if times <= 0 {
		err = fmt.Errorf("mysql: the number of transactions executed by the database has been used up")
		return
	}
	for i := 0; i < times; i++ {
		err = s.Begin()
		if err != nil {
			continue
		}
		err = anonymous(s)
		if err != nil {
			_ = s.Rollback()
			continue
		}
		_ = s.Commit()
		break
	}
	return
}

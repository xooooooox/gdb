// mysql

package mysql

import (
	"database/sql"

	"github.com/xooooooox/gas/database"
)

// pool mysql connect pool
var pool *sql.DB

// SqlLog sql log handle func
var SqlLog func(prepare string, args []interface{}) = func(prepare string, args []interface{}) {}

// SqlErr sql err handle func
var SqlErr func(err error, prepare string, args []interface{}) = func(err error, prepare string, args []interface{}) {}

// PutPool put connect pool
func PutPool(db *sql.DB) {
	if db == nil {
		return
	}
	pool = db
}

// GetPool get connect pool
func GetPool() *sql.DB {
	return pool
}

// PoolQuery connect pool query
func PoolQuery(db *sql.DB, fc func(rows *sql.Rows) error, prepare string, args ...interface{}) (err error) {
	defer SqlLog(prepare, args)
	defer SqlErr(err, prepare, args)
	err = database.Query(db, fc, prepare, args...)
	return
}

// PoolExec connect pool execute
func PoolExec(db *sql.DB, prepare string, args ...interface{}) (rowsAffected int64, err error) {
	defer SqlLog(prepare, args)
	defer SqlErr(err, prepare, args)
	rowsAffected, err = database.Exec(db, prepare, args...)
	return
}

// PoolAsk transaction
func PoolAsk(db *sql.DB, ask func(tx *sql.Tx) error) (err error) {
	defer SqlErr(err, "start sql transaction", []interface{}{})
	err = database.Ask(db, ask)
	return
}

// AskQuery transaction query
func AskQuery(tx *sql.Tx, fc func(rows *sql.Rows) error, prepare string, args ...interface{}) (err error) {
	defer SqlLog(prepare, args)
	defer SqlErr(err, prepare, args)
	err = database.AskQuery(tx, fc, prepare, args...)
	return
}

// AskExec transaction execute
func AskExec(tx *sql.Tx, prepare string, args ...interface{}) (rowsAffected int64, err error) {
	defer SqlLog(prepare, args)
	defer SqlErr(err, prepare, args)
	rowsAffected, err = database.AskExec(tx, prepare, args...)
	return
}

// Query query (connect pool)
func Query(fc func(rows *sql.Rows) error, prepare string, args ...interface{}) (err error) {
	err = PoolQuery(pool, fc, prepare, args...)
	return
}

// Exec execute (connect pool)
func Exec(prepare string, args ...interface{}) (rowsAffected int64, err error) {
	rowsAffected, err = PoolExec(pool, prepare, args...)
	return
}

// Ask transaction
func Ask(ask func(tx *sql.Tx) error) (err error) {
	err = PoolAsk(pool, ask)
	return
}

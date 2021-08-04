```go

package main

import (
	"database/sql"
	"fmt"
	"github.com/xooooooox/gdb/mysql"
	"time"
)

func main() {
	var err error
	dbuser := "root"
	dbpass := "root"
	dbhost := "127.0.0.1"
	dbport := 3306
	dbname := "test"
	dbcharset := "utf8mb4"
	var db *sql.DB
	db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s",
		dbuser,
		dbpass,
		dbhost,
		dbport,
		dbname,
		dbcharset,
	))
	if err != nil {
		fmt.Printf("cannot connect to mysql service: %s\n", err.Error())
		return
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(32)
	mysql.PutPool(db)
	defer db.Close()
	bf := func(fileSuffix string) string {
		return fmt.Sprintf("./db/mysql_struct_details.%s", fileSuffix)
	}
	cf := func(fileSuffix string) string {
		return fmt.Sprintf("./db/mysql_query_details.%s", fileSuffix)
	}
	// ***.go
	_ = mysql.WriteDatabaseToGoStruct(dbname, bf("go"), "db")
	// ***.map
	_ = mysql.WriteDatabaseToGoMap(dbname, bf("map"))
	// ***.slice
	_ = mysql.WriteDatabaseToGoSlice(dbname, bf("slice"))
	// ***.scan
	_ = mysql.WriteDatabaseToGoScan(dbname, bf("scan"))
	// ***.string
	_ = mysql.WriteDatabaseToGoColumn(dbname, bf("string"))
	// ***.go
	_ = mysql.WriteDatabaseQuicklyQuery(dbname, cf("go"), "db")
	// ***.struct.define
	_ = mysql.WriteDatabaseToGoStructDefine(dbname, bf("struct.define"))
	// ***.struct.set
	_ = mysql.WriteDatabaseToGoStructSet(dbname, bf("struct.set"))
}

```
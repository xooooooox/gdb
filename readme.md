```go
package main

import (
	"flag"
	"github.com/xooooooox/gdb/mysql"
	"log"
)

var host = flag.String("h", "127.0.0.1", "mysql host")
var port = flag.Int("P", 3306, "mysql port")
var user = flag.String("u", "root", "mysql username")
var pass = flag.String("p", "root", "mysql password")
var charset = flag.String("c", "utf8mb4", "mysql charset")
var database = flag.String("n", "test", "mysql database name")
var director = flag.String("w", "./", "write director")
var pkg = flag.String("k", "db", "write package name")

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}
	err := mysql.WriteAll(*host, *port, *user, *pass, *charset, *database, *director, *pkg)
	if err != nil {
		log.Fatalln(err)
		return
	}
	log.Println("success")
}

```
```go
package main

import (
	"github.com/xooooooox/gdb/mysql"
	"log"
)

func main() {
	err := mysql.WriteAll("127.0.0.1", 3306, "root", "root", "utf8mb4", "test", "./db/", "db")
	if err != nil {
		log.Fatalln(err)
	}
}
```
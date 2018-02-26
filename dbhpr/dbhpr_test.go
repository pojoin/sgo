package dbhpr

import (
	"encoding/json"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	err := NewDB("default", "mysql", "root:123456@tcp(127.0.0.1:3306)/tspporj?charset=utf8&parseTime=true&loc=Local")
	if err != nil {
		fmt.Println(err)
	}
}

func Test_Query(t *testing.T) {
	rows, err := Query("select * from admin_usr where id>? limit 1", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(rows)
	bs, err := json.Marshal(rows)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(string(bs))
}

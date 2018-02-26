package dbhpr

import (
	"database/sql"
	"errors"
	"fmt"
)

var dbHive map[string]*sql.DB = make(map[string]*sql.DB)

var NotFoundError error = errors.New("not found row")

func NewDB(dbname, driverName, url string) error {
	db, err := sql.Open(driverName, url)
	if err != nil {
		fmt.Errorf("error: %v\n", err)
		return err
	}
	err = db.Ping()
	if err != nil {
		fmt.Errorf("error: %v\n", err)
		return err
	}
	dbHive[dbname] = db
	return nil
}

func GetDB(dbname string) (*sql.DB, error) {
	if db, ok := dbHive[dbname]; ok {
		return db, nil
	}
	return nil, errors.New(dbname + " not found!")
}

func NewHelper(dbname string) Helper {
	return &DBHelper{
		dbname: dbname,
	}
}

func Exec(sql string, args ...interface{}) (rowsAffected int64, err error) {
	h := NewHelper("default")
	return h.Exec(sql, args...)
}

func Insert(sql string, args ...interface{}) (lastInsterId int64, err error) {
	h := NewHelper("default")
	return h.Insert(sql, args...)
}

func Update(sql string, args ...interface{}) (rowsAffected int64, err error) {
	h := NewHelper("default")
	return h.Update(sql, args...)
}

func Delete(sql string, args ...interface{}) (rowsAffected int64, err error) {
	h := NewHelper("default")
	return h.Delete(sql, args...)
}

func Count(sql string, args ...interface{}) (c int64, err error) {
	h := NewHelper("default")
	return h.Count(sql, args...)
}

func IsExists(sql string, args ...interface{}) (ok bool, err error) {
	h := NewHelper("default")
	return h.IsExists(sql, args...)
}

func Get(sql string, args ...interface{}) (Row, error) {
	h := NewHelper("default")
	return h.Get(sql, args...)
}

func Query(sql string, args ...interface{}) ([]Row, error) {
	h := NewHelper("default")
	return h.Query(sql, args...)
}

func QueryPage(page *Page, sql string, args ...interface{}) error {
	h := NewHelper("default")
	return h.QueryPage(page, sql, args...)
}

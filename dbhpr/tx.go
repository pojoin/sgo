package dbhpr

import (
	bsql "database/sql"
	"fmt"
	"strings"
)

type Tx struct {
	tx *bsql.Tx
}

func (t *Tx) Commit() error {
	return t.tx.Commit()
}

func (t *Tx) Rollback() error {
	return t.tx.Rollback()
}

func (t *Tx) Exec(sql string, args ...interface{}) (rowsAffected int64, err error) {
	r, err := t.tx.Exec(sql, args...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err = r.RowsAffected()
	return
}
func (t *Tx) Insert(sql string, args ...interface{}) (lastInsterId int64, err error) {
	r, err := t.tx.Exec(sql, args...)
	if err != nil {
		return 0, err
	}
	lastInsterId, err = r.LastInsertId()
	return
}
func (t *Tx) InsertRow(tableName string, row Row) (lastInsterId int64, err error) {
	rows, err := t.tx.Query("show columns from %s", tableName)
	if err != nil {
		return 0, err
	}
	columns := Row(make(map[string]interface{}))
	for rows.Next() {
		var field, vtype string
		rows.Scan(&field, &vtype)
		columns[field] = vtype
	}
	rows.Close()

	fields := make([]string, 0)
	placeholders := make([]string, 0)
	values := make([]interface{}, 0)
	for f, v := range row {
		if columns.IsExists(f) {
			fields = append(fields, f)
			placeholders = append(placeholders, "?")
			values = append(values, v)
		}
	}
	sql := fmt.Sprintf("insert into %s(%s) values(%s)", tableName, strings.Join(fields, ","), strings.Join(placeholders, ","))
	return t.Insert(sql, values...)
}
func (t *Tx) Update(sql string, args ...interface{}) (rowsAffected int64, err error) {
	r, err := t.tx.Exec(sql, args...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err = r.RowsAffected()
	return
}
func (t *Tx) UpdateRow(tableName string, row Row) (rowsAffected int64, err error) {
	rows, err := t.tx.Query("show columns from %s", tableName)
	if err != nil {
		return 0, err
	}
	columns := Row(make(map[string]interface{}))
	for rows.Next() {
		var field, vtype string
		rows.Scan(&field, &vtype)
		columns[field] = vtype
	}
	rows.Close()
	fields := make([]interface{}, 0)
	var cond string
	idValue := make([]interface{}, 1)
	var values = make([]interface{}, 0)
	for f, v := range row {
		if columns.IsExists(f) {
			if strings.ToLower(f) == "id" {
				cond = f + "=?"
				idValue[0] = v
			}
			fields = append(fields, f+"=?")
			values = append(values, v)
		}
	}
	values = append(values, idValue[0])
	sql := fmt.Sprintf("update %s set %s where %s", tableName, fields, cond)
	return t.Update(sql, values)
}
func (t *Tx) Delete(sql string, args ...interface{}) (rowsAffected int64, err error) {
	return t.Update(sql, args...)
}
func (t *Tx) Count(sql string, args ...interface{}) (c int64, err error) {
	tmpsql := strings.ToUpper(sql)
	if fromIndex := strings.Index(tmpsql, "FROM "); fromIndex > 0 {
		sql = fmt.Sprintf("select count(*) %s", []byte(sql)[fromIndex:])
	}
	r := t.tx.QueryRow(sql, args...)
	err = r.Scan(&c)
	return c, err
}
func (t *Tx) IsExists(sql string, args ...interface{}) (ok bool, err error) {
	c, err := t.Count(sql, args...)
	if err != nil {
		return false, err
	}
	if c > 0 {
		return true, err
	}
	return false, err
}
func (t *Tx) Get(sql string, args ...interface{}) (Row, error) {
	if !strings.Contains(strings.ToLower(sql), "limit") {
		sql += " limit 1 "
	}

	rows, err := t.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, NotFoundError
	}
	return rows[0], nil
}
func (t *Tx) Query(sql string, args ...interface{}) ([]Row, error) {
	rows, err := t.tx.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := make([]Row, 0)
	err = parseResult(rows, &results)
	return results, err
}
func (t *Tx) QueryPage(page *Page, sql string, args ...interface{}) error {
	//get count
	count, err := t.Count(sql, args...)
	if err != nil {
		return err
	}
	page.Count = count
	if count == 0 {
		page.List = make([]Row, 0)
		return nil
	}

	sql = fmt.Sprintf("%s limit %d,%d", sql, page.StartRow(), page.PageSize)

	//query rows
	rows, err := t.tx.Query(sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	results := make([]Row, 0, page.Count)
	err = parseResult(rows, &results)
	page.List = results
	return err
}

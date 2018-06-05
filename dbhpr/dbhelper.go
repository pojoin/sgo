package dbhpr

import (
	bsql "database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type DBHelper struct {
	dbname string
}

func (h *DBHelper) Exec(sql string, args ...interface{}) (rowsAffected int64, err error) {
	stmt, err := dbHive[h.dbname].Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	r, err := stmt.Exec(args...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err = r.RowsAffected()
	return
}

func (h *DBHelper) Insert(sql string, args ...interface{}) (lastInsterId int64, err error) {
	stmt, err := dbHive[h.dbname].Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	r, err := stmt.Exec(args...)
	if err != nil {
		return 0, err
	}
	lastInsterId, err = r.LastInsertId()
	return
}

func (h *DBHelper) Update(sql string, args ...interface{}) (rowsAffected int64, err error) {
	stmt, err := dbHive[h.dbname].Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	r, err := stmt.Exec(args...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err = r.RowsAffected()
	return
}

func (h *DBHelper) Delete(sql string, args ...interface{}) (rowsAffected int64, err error) {
	return h.Update(sql, args...)
}

func (h *DBHelper) Get(sql string, args ...interface{}) (Row, error) {
	if !strings.Contains(strings.ToLower(sql), "limit") {
		sql += " limit 1 "
	}

	rows, err := h.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, NotFoundError
	}
	return rows[0], nil
}

func (h *DBHelper) Query(sql string, args ...interface{}) ([]Row, error) {
	// fmt.Println("sql = ", sql, args)
	stmt, err := dbHive[h.dbname].Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := make([]Row, 0)
	err = parseResult(rows, &results)
	return results, err
}

func (h *DBHelper) IsExists(sql string, args ...interface{}) (ok bool, err error) {
	c, err := h.Count(sql, args...)
	if err != nil {
		return false, err
	}
	if c > 0 {
		return true, err
	}
	return false, err
}

func (h *DBHelper) Count(sql string, args ...interface{}) (c int64, err error) {
	// if tmpsql := strings.ToUpper(sql); !strings.Contains(tmpsql, "COUNT(") {
	// 	if fromIndex := strings.Index(tmpsql, "FROM"); fromIndex > 0 {
	// 		sql = fmt.Sprintf("select count(*) %s", []byte(sql)[fromIndex:])
	// 	}
	// }
	tmpsql := strings.ToUpper(sql)
	if fromIndex := strings.Index(tmpsql, "FROM "); fromIndex > 0 {
		sql = fmt.Sprintf("select count(*) %s", []byte(sql)[fromIndex:])
	}
	r := dbHive[h.dbname].QueryRow(sql, args...)
	err = r.Scan(&c)
	return c, err
}

func (h *DBHelper) QueryPage(page *Page, sql string, args ...interface{}) error {
	//get count
	count, err := h.Count(sql, args...)
	if err != nil {
		return err
	}
	page.Count = count
	if count == 0 {
		page.List = make([]Row, 0)
		return nil
	}

	sql = fmt.Sprintf("%s limit %d,%d", sql, page.StartRow(), page.PageSize)

	//stmt
	stmt, err := dbHive[h.dbname].Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	//query rows
	rows, err := stmt.Query(args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	results := make([]Row, 0, page.Count)
	err = parseResult(rows, &results)
	page.List = results
	return err
}

func parseResult(rows *bsql.Rows, results *[]Row) error {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	for rows.Next() {
		row := make(map[string]interface{})
		values := make([]interface{}, 0, len(columnTypes))
		for _, t := range columnTypes {
			// fmt.Println("name=", t.Name(), ",type=", t.ScanType(), ",databaseTypeName=", t.DatabaseTypeName())
			values = append(values, reflect.New(t.ScanType()).Interface())
		}
		err = rows.Scan(values...)
		if err != nil {
			return err
		}

		for i, t := range columnTypes {
			value := reflect.Indirect(reflect.ValueOf(values[i])).Interface()
			// fmt.Println(reflect.TypeOf(value))
			switch v := value.(type) {
			case bsql.RawBytes:
				row[t.Name()] = string(v)
			case bsql.NullInt64:
				if v.Valid {
					row[t.Name()] = v.Int64
				} else {
					row[t.Name()] = 0
				}
			case bsql.NullBool:
				if v.Valid {
					row[t.Name()] = v.Bool
				} else {
					row[t.Name()] = false
				}
			case bsql.NullFloat64:
				if v.Valid {
					row[t.Name()] = v.Float64
				} else {
					row[t.Name()] = 0.0
				}
			case bsql.NullString:
				if v.Valid {
					row[t.Name()] = v.String
				} else {
					row[t.Name()] = ""
				}
			case time.Time:
				row[t.Name()] = Time(v)
			case mysql.NullTime:
				if v.Valid {
					row[t.Name()] = Time(v.Time)
				} else {
					row[t.Name()] = ""
				}
			default:
				row[t.Name()] = v
			}

		}
		*results = append(*results, row)
	}
	return err
}

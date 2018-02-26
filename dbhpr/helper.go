package dbhpr

import (
	"errors"
	"time"
)

const (
	timeFormart = "2006-01-02 15:04:05"
)

type Row map[string]interface{}

func (r Row) GetInt64(col string) int64 {
	var value int64
	switch v := r[col].(type) {
	case int8:
		value = int64(v)
	case int16:
		value = int64(v)
	case int32:
		value = int64(v)
	case int:
		value = int64(v)
	case int64:
		value = v
	}
	return value
}

func (r Row) GetInt(col string) int {
	return int(r.GetInt64(col))
}

func (r Row) GetString(col string) string {
	v, _ := r[col].(string)
	return v
}

func (r Row) Append(col string, v interface{}) error {
	m := map[string]interface{}(r)
	if _, ok := m[col]; ok {
		return errors.New("Append [" + col + "] is exists")
	}
	m[col] = v
	return nil
}

type Time time.Time

func (t *Time) UnmarshalJSON(data []byte) (err error) {
	now, err := time.ParseInLocation(`"`+timeFormart+`"`, string(data), time.Local)
	*t = Time(now)
	return
}

func (t Time) MarshalJSON() ([]byte, error) {
	b := make([]byte, 0, len(timeFormart)+2)
	b = append(b, '"')
	b = time.Time(t).AppendFormat(b, timeFormart)
	b = append(b, '"')
	return b, nil
}

func (t Time) String() string {
	return time.Time(t).Format(timeFormart)
}

type Helper interface {
	Exec(sql string, args ...interface{}) (rowsAffected int64, err error)
	Insert(sql string, args ...interface{}) (lastInsterId int64, err error)
	Update(sql string, args ...interface{}) (rowsAffected int64, err error)
	Delete(sql string, args ...interface{}) (rowsAffected int64, err error)
	Count(sql string, args ...interface{}) (c int64, err error)
	IsExists(sql string, args ...interface{}) (ok bool, err error)
	Get(sql string, args ...interface{}) (Row, error)
	Query(sql string, args ...interface{}) ([]Row, error)
	QueryPage(page *Page, sql string, args ...interface{}) error
}

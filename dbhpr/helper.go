package dbhpr

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

const (
	timeFormart = "2006-01-02 15:04:05"
)

type Row map[string]interface{}

func (r Row) IsExists(key string) bool {
	if _, ok := r[key]; ok {
		return true
	}
	return false
}

func (r Row) GetInt64(col string) int64 {
	var value int64
	switch v := r[col].(type) {
	case int8:
		value = int64(v)
	case uint:
		value = int64(v)
	case uint16:
		value = int64(v)
	case int16:
		value = int64(v)
	case int32:
		value = int64(v)
	case uint32:
		value = int64(v)
	case uint8:
		value = int64(v)
	case uint64:
		value = int64(v)
	case int:
		value = int64(v)
	case int64:
		value = v
	case string:
		v1, err := strconv.Atoi(v)
		if err != nil {
			fmt.Println("dbhpr error:", err)
			return 0
		}
		value = int64(v1)

	}
	return value
}

func (r Row) GetUint64(col string) uint64 {
	return uint64(r.GetInt64(col))
}

func (r Row) GetInt8(col string) int8 {
	return int8(r.GetInt64(col))
}

func (r Row) GetUint8(col string) uint8 {
	return uint8(r.GetInt64(col))
}

func (r Row) GetInt16(col string) int16 {
	return int16(r.GetInt64(col))
}

func (r Row) GetUint16(col string) uint16 {
	return uint16(r.GetInt64(col))
}

func (r Row) GetInt32(col string) int32 {
	return int32(r.GetInt64(col))
}

func (r Row) GetUint32(col string) uint32 {
	return uint32(r.GetInt64(col))
}

func (r Row) GetUint(col string) uint {
	return uint(r.GetInt64(col))
}

func (r Row) GetInt(col string) int {
	return int(r.GetInt64(col))
}

func (r Row) GetString(col string) string {
	v, _ := r[col].(string)
	return v
}

func (r Row) GetFloat64(col string) float64 {
	var value float64
	switch v := r[col].(type) {
	case float32:
		value = float64(v)
	case float64:
		value = float64(v)
	}
	return value
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

func (t Time) Format(df string) string {
	return time.Time(t).Format(df)
}

type Helper interface {
	Exec(sql string, args ...interface{}) (rowsAffected int64, err error)
	Insert(sql string, args ...interface{}) (lastInsterId int64, err error)
	InsertRow(tableName string, row Row) (lastInsterId int64, err error)
	Update(sql string, args ...interface{}) (rowsAffected int64, err error)
	UpdateRow(tableName string, row Row) (rowsAffected int64, err error)
	Delete(sql string, args ...interface{}) (rowsAffected int64, err error)
	Count(sql string, args ...interface{}) (c int64, err error)
	IsExists(sql string, args ...interface{}) (ok bool, err error)
	Get(sql string, args ...interface{}) (Row, error)
	Query(sql string, args ...interface{}) ([]Row, error)
	QueryPage(page *Page, sql string, args ...interface{}) error
}

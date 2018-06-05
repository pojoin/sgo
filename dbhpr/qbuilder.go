package dbhpr

import (
	"fmt"
	"strings"
)

var ()

type QBuilder struct {
	columnstr string //select field
	tableName string //table
	filters   string //condition
	orderby   string //orderby
	groupby   string //groupby
	limit     string //limit
	join      string //join
}

func (q *QBuilder) Select(fields ...string) *QBuilder {
	q.columnstr = strings.Join(fields, ",")
	return q
}

func (q *QBuilder) From(tableName ...string) *QBuilder {
	q.tableName = strings.Join(tableName, " ")
	return q
}

func (q *QBuilder) Where(param ...interface{}) *QBuilder {
	for _, p := range param {
		q.filters += fmt.Sprintf(" %v ", p)
	}
	return q
}

func (q *QBuilder) Filter(param ...interface{}) *QBuilder {
	for _, p := range param {
		q.filters += fmt.Sprintf(" %v ", p)
	}
	return q
}

func (q *QBuilder) And(condition string, args ...interface{}) *QBuilder {
	condition = " and " + condition
	q.filters += fmt.Sprintf(condition, args...)
	return q
}

func (q *QBuilder) Or(condition string, args ...interface{}) *QBuilder {
	condition = " or (" + condition + ") "
	q.filters += fmt.Sprintf(condition, args...)
	return q
}

func (q *QBuilder) GroupBy(param string) *QBuilder {
	q.groupby = fmt.Sprintf("GROUP BY %v", param)
	return q
}

//orderBy
func (q *QBuilder) OrderBy(param string) *QBuilder {
	q.orderby = fmt.Sprintf("ORDER By %v", param)
	return q
}

//limit
func (q *QBuilder) Limit(size ...int) *QBuilder {
	if len(size) > 1 {
		q.limit = fmt.Sprintf("Limit %d,%d", size[0], size[1])
		return q
	} else {
		q.limit = fmt.Sprintf("Limit %d", size[0])
		return q
	}
}

func (q *QBuilder) LeftJoin(table string) *QBuilder {
	q.join += fmt.Sprintf(" left join %v ", table)
	return q
}

func (q *QBuilder) RightJoin(table string) *QBuilder {
	q.join += fmt.Sprintf(" right join %v ", table)
	return q
}

func (q *QBuilder) Join(table string) *QBuilder {
	q.join += fmt.Sprintf(" join %v ", table)
	return q
}

func (q *QBuilder) InnerJoin(table string) *QBuilder {
	q.join += fmt.Sprintf(" inner Join %v ", table)
	return q
}

func (q *QBuilder) buildSql(columnstr string) string {
	where := q.filters
	where = strings.TrimSpace(where)
	if len(where) > 0 {
		where = "where " + where
	}
	query := fmt.Sprintf("select %v from %v %v %v %v %v", columnstr, q.tableName, q.join, where, q.groupby, q.orderby)
	return query
}

func (q *QBuilder) Csql() string {
	return q.buildSql("count(*)")
}

func (q *QBuilder) Sql() string {
	return q.buildSql(q.columnstr) + q.limit
}

func (q *QBuilder) Query(dbname ...string) ([]Row, error) {
	dn := "default"
	if len(dbname) > 0 {
		dn = dbname[0]
	}
	stmt, err := dbHive[dn].Prepare(q.Sql())
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := make([]Row, 0)
	err = parseResult(rows, &results)
	return results, err
}

func (q *QBuilder) QueryPage(page *Page, dbname ...string) error {
	dn := "default"
	if len(dbname) > 0 {
		dn = dbname[0]
	}
	db := dbHive[dn]
	var count int64
	r := db.QueryRow(q.Csql())
	err := r.Scan(&count)
	page.Count = count
	if count == 0 {
		page.List = make([]Row, 0)
		return nil
	}
	q.Limit(page.StartRow(), page.PageSize)
	//stmt
	stmt, err := db.Prepare(q.Sql())
	if err != nil {
		return err
	}
	defer stmt.Close()

	//query rows
	rows, err := stmt.Query()
	if err != nil {
		return err
	}
	defer rows.Close()
	results := make([]Row, 0, count)
	err = parseResult(rows, &results)
	page.List = results
	return err

}

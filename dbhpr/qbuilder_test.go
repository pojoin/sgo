package dbhpr

import (
	"testing"

	"github.com/pojoin/sgo/dbhpr"
)

func Test_BuildSql(t *testing.T) {
	qb := dbhpr.QBuilder{}
	qb.Select("id,name,pwd,avta").From("user u")
	qb.LeftJoin("dept d on u.deptId=d.id")
	qb.Where("1=1")
	qb.And("name like '%%%s%%'", "hcq")
	qb.And("pwd='%s'", "hcq")
	qb.Limit(10)
	qb.GroupBy("id desc")
	t.Log(qb.Sql())
}

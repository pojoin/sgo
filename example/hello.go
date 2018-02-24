package main

import (
	"io/ioutil"
	"log"
	"os"
)

type loginFilter struct{}

func (l *loginFilter) Execute(ctx *sgo.Context) (ok bool) {
	ok = false
	log.Println("loginFilter")
	ctx.WriteJson("没有权限")
	return
}

func main() {

	sgo.AddFilter(&loginFilter{})

	sgo.AddRoute("GET", "/hello/:name/ok/", func(ctx *sgo.Context) {
		ctx.WriteJson("hello , " + ctx.Params["name"])
	})

	sgo.AddRoute("GET", "/test/", func(ctx *sgo.Context) {
		ctx.Data["name"] = "张三"
		ctx.WriteTpl("text.html")
	})

	sgo.AddRoute("GET", "/user/", func(ctx *sgo.Context) {
		ctx.Data["users"] = []string{"张三", "李四", "王五"}
		ctx.WriteTpl("user/user.htm")
	})

	sgo.AddRoute("GET", "/redirct/", func(ctx *sgo.Context) {
		ctx.Redirect("/user")
	})

	sgo.AddRoute("GET", "/panic/", func(ctx *sgo.Context) {
		panic("ok")
	})

	sgo.AddRoute("GET", "/download/", func(ctx *sgo.Context) {
		f, err := os.Open("hello.go")
		if err != nil {
			ctx.Abort(500, "open file fail")
			return
		}
		defer f.Close()
		buf, _ := ioutil.ReadAll(f)
		ctx.WriteStream("1t.html", "application/octet-stream", buf)
	})

	sgo.RunSimpleServer(":9000")
}

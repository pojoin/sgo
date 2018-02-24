package ak

import (
	"log"
	"net/http"
	"os"
	"path"
)

var simpleServer = NewDefaultServer()

//给simpleServer添加路由
func AddRoute(methods int, url string, f actionFunc) {
	simpleServer.AddRoute(methods, url, f)
}

//给simpleServer添加过滤器
func AddFilter(filter Filter) {
	simpleServer.AddFilter(filter)
}

//给simpleServer添加静态文件夹
func AddStaticDir(dir string) {
	simpleServer.AddStaticDir(dir)
}

//给simpleServer设置模板路径
func SetTplPath(tplPath string) {
	simpleServer.SetTplPath(tplPath)
}

//给simpleServer设置模板标签边界
func SetTplDelim(leftDelim, rightDelim string) {
	simpleServer.SetTplDelim(leftDelim, rightDelim)
}

//开启session处理
func StartSession(state bool) {
	simpleServer.StartSession(state)
}

//创建默认server
func NewDefaultServer() *Server {
	wd, _ := os.Getwd()
	cfg := &serverConfig{}
	cfg.basePath = wd
	cfg.sessionProc = false
	cfg.leftDelim = "{{"
	cfg.rightDelim = "}}"
	cfg.defaultStaticDirs = append(cfg.defaultStaticDirs, path.Join(wd, "web"))
	cfg.tplPath = path.Join(wd, "web")
	return &Server{config: cfg, filterChain: make([]Filter, 0), router: newRouter()}
}

//启动simpleServer服务
func RunSimpleServer(addr string) {
	log.Println("webServer addr	:", "[", addr, "]")
	log.Println("tplPath		:", simpleServer.config.tplPath)
	log.Println("staticDir		:", simpleServer.config.defaultStaticDirs)
	mux := http.NewServeMux()
	mux.Handle("/", simpleServer)
	err := http.ListenAndServe(addr, mux)
	if err != nil {
		log.Fatal(err)
	}
}

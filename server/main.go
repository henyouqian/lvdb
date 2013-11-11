package main

import (
	"fmt"
	"github.com/henyouqian/lvdb"
	"net"
	"net/http"
	"net/rpc"
	"runtime"
)

// func main() {
// 	runtime.GOMAXPROCS(runtime.NumCPU())

// 	lvdb := new(lvdb.Lvdb)
// 	server := rpc.NewServer()
// 	server.Register(lvdb)

// 	l, err := net.Listen("tcp", ":1234")
// 	if err != nil {
// 		fmt.Println("监听失败，端口可能已经被占用")
// 	}
// 	fmt.Println("正在监听1234端口")
// 	server.Accept(l)
// }

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	db, err := lvDB.InitLvDB()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	lvdb := new(lvDB.Lvdb)
	rpc.Register(lvdb)
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		fmt.Println("监听失败，端口可能已经被占用")
	}
	fmt.Println("正在监听1234端口")
	http.Serve(l, nil)
}

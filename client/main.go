package main

import (
	"flag"
	"fmt"
	"github.com/golang/glog"
	"github.com/henyouqian/lvdb"
	// "io"
	// "net"
	"net/rpc"
	"sync"
	"time"
)

func main() {
	flag.Parse()
	t := time.Now()

	client, err := rpc.DialHTTP("tcp", "127.0.0.1:1234")
	if err != nil {
		fmt.Println("链接rpc服务器失败:", err)
	}

	// address, err := net.ResolveTCPAddr("tcp", "localhost:1234")
	// if err != nil {
	// 	fmt.Println("链接rpc服务器失败:", err)
	// }

	var wg sync.WaitGroup

	f := func(n int) {
		defer wg.Done()

		// conn, err := net.DialTCP("tcp", nil, address)
		// if err != nil {
		// 	fmt.Println("DialTCP error", err)
		// 	return
		// }
		// defer conn.Close()

		// client := rpc.NewClient(conn)
		// defer client.Close()

		for i := 0; i < 5000; i++ {
			in := []lvDB.Kv{
				{[]byte("aa"), []byte("44")},
				{[]byte("bb"), []byte("89")},
			}
			var changedNum int
			err = client.Call("Lvdb.Put", &in, &changedNum)
			if err != nil {
				fmt.Println("调用远程服务失败", err)
			} else {
				fmt.Println("远程服务返回结果：", changedNum)
			}

			aa := [][]byte{
				[]byte("aa"),
				[]byte("bb"),
			}
			var out [][]byte
			err = client.Call("Lvdb.Get", aa, &out)
			glog.Errorln(out, err)

			time.Sleep(time.Second)
		}
	}

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go f(i)
	}

	wg.Wait()

	dt := time.Now().Sub(t).Seconds()
	glog.Infoln(dt)
}

package lvDB

import (
	"container/list"
	"errors"
	//"github.com/golang/glog"
	"net/rpc"
	"sync"
)

var ErrClientBroken = errors.New("lvDB: client broken")

type Pool struct {
	Url     string
	MaxIdle uint32
	//IdleTimeout time.Time

	mu          sync.Mutex
	idleClients list.List
}

func NewPool(url string, maxIdel uint32) *Pool {
	pool := Pool{
		Url:     url,
		MaxIdle: maxIdel,
	}
	pool.idleClients.Init()
	return &pool
}

func (p *Pool) Get() (*Client, error) {
	p.mu.Lock()
	e := p.idleClients.Front()
	if e != nil {
		p.idleClients.Remove(e)
		p.mu.Unlock()
		return &Client{p, e.Value.(*rpc.Client), false}, nil
	}
	p.mu.Unlock()

	//create new
	client, err := rpc.DialHTTP("tcp", p.Url)
	if err != nil {
		return nil, err
	}

	return &Client{p, client, false}, nil
}

func (p *Pool) Put(client *Client) {
	p.mu.Lock()
	p.idleClients.PushBack(client.client)
	if p.idleClients.Len() > int(p.MaxIdle) {
		e := p.idleClients.Front()
		e.Value.(*rpc.Client).Close()
		p.idleClients.Remove(e)
	}
	p.mu.Unlock()
}

type Client struct {
	pool   *Pool
	client *rpc.Client
	broken bool
}

func (c *Client) Close() {
	if c.broken {
		c.client.Close()
		return
	}
	c.pool.Put(c)
}

func (c *Client) Put(kvs ...Kv) error {
	err := c.client.Call("Lvdb.Put", kvs, nil)
	if err == rpc.ErrShutdown {
		c.broken = true
		return ErrClientBroken
	}
	return err
}

func (c *Client) Get(keys ...[]byte) (replys [][]byte, err error) {
	err = c.client.Call("Lvdb.Get", keys, &replys)
	if err == rpc.ErrShutdown {
		c.broken = true
		return nil, ErrClientBroken
	}
	return replys, err
}

func (c *Client) Del(keys ...[]byte) error {
	err := c.client.Call("Lvdb.Del", keys, nil)
	if err == rpc.ErrShutdown {
		c.broken = true
		return ErrClientBroken
	}
	return err
}

//func (c *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
//	err := c.client.Call(serviceMethod, args, reply)
//	if err == rpc.ErrShutdown {
//		c.broken = true
//		return ErrClientBroken
//	}

//	return err
//}

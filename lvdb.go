package lvDB

import (
	// "fmt"
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	db *leveldb.DB
)

type Lvdb int

func InitLvDB() (*leveldb.DB, error) {
	var err error
	db, err = leveldb.OpenFile("db", nil)
	return db, err
}

type Kv struct {
	Key   interface{}
	Value interface{}
}

func (_ *Lvdb) Set(arg *Kv, reply *bool) error {
	*reply = true
	return nil
}

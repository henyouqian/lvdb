package main

import (
	// "fmt"
	"errors"
	// "github.com/golang/glog"
	"github.com/henyouqian/lvdb"
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

func (_ *Lvdb) Put(kvs []lvDB.Kv, changedNum *int) error {
	*changedNum = 0
	n := len(kvs)
	if n == 0 {
		return errors.New("empty kvs")
	}
	if n == 1 {
		kv := kvs[0]
		if err := db.Put(kv.Key, kv.Value, nil); err != nil {
			return err
		}
	} else {
		batch := new(leveldb.Batch)
		for _, kv := range kvs {
			batch.Put(kv.Key, kv.Value)
		}
		if err := db.Write(batch, nil); err != nil {
			return err
		}
	}
	*changedNum = n
	return nil
}

func (_ *Lvdb) Get(ks [][]byte, vs *[][]byte) error {
	for _, k := range ks {
		if v, err := db.Get(k, nil); err != nil {
			if err == leveldb.ErrNotFound {
				*vs = append(*vs, nil)
			} else {
				return err
			}
		} else {
			*vs = append(*vs, v)
		}

	}
	return nil
}

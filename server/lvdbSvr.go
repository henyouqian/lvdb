package main

import (
	// "fmt"
	"encoding/json"
	"errors"
	"github.com/golang/glog"
	"github.com/henyouqian/lvdb"
	"github.com/robfig/cron"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"time"
)

var (
	db *leveldb.DB
	cf conf
)

type Lvdb int

func InitLvDB() (*leveldb.DB, error) {
	var err error

	if err = loadConf(); err != nil {
		return nil, err
	}

	backupTask()

	db, err = leveldb.OpenFile(cf.DbName, nil)
	return db, err
}

func (_ *Lvdb) Put(kvs []lvDB.Kv, changedNum *int) error {
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

//fixme: db.Delete has no effect
func (_ *Lvdb) Del(ks [][]byte, delNum *int) error {
	n := len(ks)
	if n == 0 {
		return errors.New("empty keys")
	}
	if n == 1 {
		k := ks[0]
		if err := db.Delete(k, nil); err != nil {
			return err
		}
	} else {
		batch := new(leveldb.Batch)
		for _, k := range ks {
			batch.Delete(k)
		}
		if err := db.Write(batch, nil); err != nil {
			return err
		}
	}
	*delNum = n
	return nil
}

type conf struct {
	DbName     string
	BackupCron string
}

func loadConf() error {
	var f *os.File
	var err error

	if f, err = os.Open("lvdb.conf"); err != nil {
		return err
	}

	decoder := json.NewDecoder(f)
	if err = decoder.Decode(&cf); err != nil {
		return err
	}

	glog.Infof("%+v\n", cf)

	return nil
}

func backup() error {
	dbName := "db." + time.Now().Format("2006-01-02T15-04-05Z07-00")
	backupDb, err := leveldb.OpenFile(dbName, nil)
	if err != nil {
		return err
	}
	defer backupDb.Close()

	snapshot, err := db.GetSnapshot()
	if err != nil {
		return err
	}
	defer snapshot.Release()

	iter := snapshot.NewIterator(nil)
	for iter.Next() {
		err = backupDb.Put(iter.Key(), iter.Value(), nil)
		if err != nil {
			return err
		}
	}

	return iter.Error()
}

func backupTask() {
	c := cron.New()
	c.AddFunc(cf.BackupCron, func() { backup() })
	c.Start()
}

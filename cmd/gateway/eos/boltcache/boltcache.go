package eos

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/boltdb/bolt"
	"github.com/minio/minio/cmd/logger"
	"os"
)

// BoltStatCache is used for storing a cache of FileStat's per HTTP request
type BoltStatCache struct {
	path    string
	dbstore string
	db      *bolt.DB
}

// NewBoltStatCache creates a new BoltCache
func NewBoltStatCache(path string) *BoltStatCache {
	b := &BoltStatCache{}
	b.path = path
	b.dbstore = "/stage/statcache.db" // TODO: come back and fix this
	b.db, _ = b.setupDB(b.dbstore)
	return b
}

// setup
func (b *BoltStatCache) setupDB(path string) (db *bolt.DB, err error) {
	if _, err := os.Stat(b.dbstore); os.IsExist(err) {
		err = os.Remove(b.dbstore)
		if err != nil {
			return nil, err
		}
	}
	db, err = bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, errors.New("unable to open boltstatcache db")
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("STATCACHE"))
		if err != nil {
			return errors.New("could not create root bucket")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Write -
func (b *BoltStatCache) Write(ctx context.Context, key string, stat *FileStat) (err error) {
	reqID := b.getReqID(ctx)
	err = b.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.Bucket([]byte("STATCACHE")).CreateBucketIfNotExists([]byte(reqID))
		if err != nil {
			return errors.New("unable to create bucket")
		}

		statJSON, err := json.Marshal(stat)
		if err != nil {
			return err
		}
		eosLogger.Debug(ctx, "BoltStatCache.Write", "Write. key: %s val: %s stat: %+v", key, statJSON, stat)
		err = tx.Bucket([]byte("STATCACHE")).Bucket([]byte(reqID)).Put([]byte(key), statJSON)
		if err != nil {
			eosLogger.Debug(ctx, "BoltStatCache.Write", "Write cache failed. key: %s val: %s", key, statJSON)
			return err
		}
		return nil
	})
	if err != nil {
		eosLogger.Debug(ctx, "BoltStatCache.Write", "Write failed. key: %s", key)
		return err
	}
	return nil
}

// Read -
func (b *BoltStatCache) Read(ctx context.Context, key string) (stat *FileStat, err error) {
	reqID := b.getReqID(ctx)
	err = b.db.View(func(tx *bolt.Tx) (err error) {
		bucket := tx.Bucket([]byte("STATCACHE")).Bucket([]byte(reqID))
		if bucket == nil {
			return errors.New("bucket not found")
		}
		statJSON := bucket.Get([]byte(key))
		eosLogger.Debug(ctx, "BoltStatCache.Read", "statJSON for %s: %s", key, statJSON)
		var cachedStat FileStat
		err = json.Unmarshal(statJSON, &cachedStat)
		if err != nil {
			return err
		}
		stat = &cachedStat
		return nil
	})
	if err != nil {
		eosLogger.Debug(ctx, "BoltStatCache.Read", "Error reading key: %s err: %+v", key, err)
		return nil, err
	}
	eosLogger.Debug(ctx, "BoltStatCache.Read", "Read key: %s, stat: %+v", key, stat)
	return stat, nil
}

// Delete -
func (b *BoltStatCache) Delete(ctx context.Context, key string) (err error) {
	reqID := b.getReqID(ctx)
	err = b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("STATCACHE")).Bucket([]byte(reqID))
		if bucket == nil {
			return errors.New("bucket not found")
		}
		bucket.Delete([]byte(reqID))
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// DeleteCache -
func (b *BoltStatCache) DeleteCache(ctx context.Context) error {
	reqID := b.getReqID(ctx)
	err := b.db.Update(func(tx *bolt.Tx) (err error) {
		bucket := tx.Bucket([]byte("STATCACHE")).Bucket([]byte(reqID))
		if bucket != nil {
			if err = tx.Bucket([]byte("STATCACHE")).DeleteBucket([]byte(reqID)); err != nil {
				return errors.New("unable to delete bucket")
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// getReqID returns the request ID associated with the context
func (b *BoltStatCache) getReqID(ctx context.Context) string {
	reqInfo := logger.GetReqInfo(ctx)
	reqID := "none"
	if reqInfo.RequestID != "" {
		reqID = reqInfo.RequestID
	}
	return reqID
}

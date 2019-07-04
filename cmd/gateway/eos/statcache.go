/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 * Michael D'Silva
 *
 */

package eos

import (
	"path/filepath"
	"strings"
	"sync"
)

type StatCache struct {
	sync.RWMutex
	path  string
	cache map[string]eosFileStat
}

func NewStatCache(path string) *StatCache {
	c := new(StatCache)
	c.path = path
	c.cache = make(map[string]eosFileStat)
	return c
}

func (c *StatCache) Reset() {
	c.Lock()
	c.cache = make(map[string]eosFileStat)
	c.Unlock()
}

func (c *StatCache) Read(path string) (*eosFileStat, bool) {
	c.RLock()
	fi, ok := c.cache[path]
	c.RUnlock()
	if ok {
		return &fi, true
	}
	return nil, false
}

func (c *StatCache) Write(path string, obj eosFileStat) {
	c.Lock()
	c.cache[path] = obj
	c.Unlock()
}

func (c *StatCache) DeletePath(path string) {
	c.Lock()
	if _, ok := c.cache[path]; ok {
		delete(c.cache, path)
	}
	c.Unlock()
}

func (c *StatCache) DeleteObject(bucket, object string) {
	path, err := c.AbsoluteEOSPath(bucket + "/" + object)
	if err == nil {
		c.DeletePath(path)
	}
}

// TODO: should be a global helper method, not tied to a type - this is the only reason we have a "path" property on StatCache.
func (c *StatCache) AbsoluteEOSPath(path string) (eosPath string, err error) {
	if strings.Contains(path, "..") {
		return "", eoserrFilePathBad
	}

	path = strings.Replace(path, "//", "/", -1)
	eosPath = strings.TrimSuffix(c.path+"/"+path, ".")
	eosPath = filepath.Clean(eosPath)
	return eosPath, nil
}

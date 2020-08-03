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

// StatCache is a lockable map of FileStat's
type StatCache struct {
	sync.RWMutex
	path  string
	cache map[string]*FileStat
}

// NewStatCache creates a new StatCache and returns it
func NewStatCache(path string) *StatCache {
	c := new(StatCache)
	c.path = path
	c.cache = make(map[string]*FileStat)
	return c
}

// Size returns the len() of the cache
func (c *StatCache) Size() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.cache)
}

// Reset deletes and recreates the StatCache's cache
func (c *StatCache) Reset() {
	c.Lock()
	for key := range c.cache {
		delete(c.cache, key)
	}
	c.cache = make(map[string]*FileStat)
	c.Unlock()
}

// Read tries to read an entry from the StatCache
func (c *StatCache) Read(path string) (*FileStat, bool) {
	c.RLock()
	fi, ok := c.cache[path]
	c.RUnlock()
	if ok {
		return fi, true
	}
	return nil, false
}

// Write creates a new entry in the StatCache
func (c *StatCache) Write(path string, obj *FileStat) {
	if obj.IsDir() && !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	c.Lock()
	c.cache[path] = obj
	c.Unlock()
}

// DeletePath remove an entry from the StatCache
func (c *StatCache) DeletePath(path string) {
	c.Lock()
	delete(c.cache, path)
	c.Unlock()
}

// DeleteObject deletes an object from the StatCache
func (c *StatCache) DeleteObject(bucket, object string) {
	path, err := c.AbsoluteEOSPath(PathJoin(bucket, object))
	if err == nil {
		c.DeletePath(path)
	}
}

// AbsoluteEOSPath cleans up and returns a full EOS path
func (c *StatCache) AbsoluteEOSPath(path string) (eosPath string, err error) {
	// TODO: should be a global helper method, not tied to a type - this is the only reason we have a "path" property on StatCache.
	if strings.Contains(path, "..") {
		return "", errFilePathBad
	}

	path = strings.Replace(path, "//", "/", -1)
	eosPath = strings.TrimSuffix(PathJoin(c.path, path), ".")
	eosPath = filepath.Clean(eosPath)
	return eosPath, nil
}

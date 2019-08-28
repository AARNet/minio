package eos

import (
	"context"
	"github.com/minio/minio/cmd/logger"
	"runtime/debug"
	"sync"
)

// RequestStatCache is used for storing a cache of FileStat's per HTTP request
type RequestStatCache struct {
	sync.RWMutex
	path  string
	cache map[string]*StatCache
}

// NewRequestStatCache creates a new RequestStatCache
func NewRequestStatCache(path string) *RequestStatCache {
	rc := &RequestStatCache{}
	rc.path = path
	rc.cache = make(map[string]*StatCache)
	return rc
}

// init just initializes the cache if it doesn't exist
func (c *RequestStatCache) init() {
	c.Lock()
	if c.cache == nil {
		c.cache = make(map[string]*StatCache)
	}
	c.Unlock()
}

// Get returns an already existing StatCache or creates a new one and returns it
func (c *RequestStatCache) Get(ctx context.Context) (result *StatCache) {
	reqInfo := logger.GetReqInfo(ctx)
	reqID := "none"
	if reqInfo.RequestID != "" {
		reqID = reqInfo.RequestID
	}
	c.init()
	c.RLock()
	if _, ok := c.cache[reqID]; !ok {
		c.RUnlock()
		c.Lock()
		c.cache[reqID] = NewStatCache(c.path)
		c.Unlock()
		c.RLock()
	}
	result = c.cache[reqID]
	c.RUnlock()
	return result
}

// Reset deletes and recreates a StatCache
func (c *RequestStatCache) Reset(ctx context.Context) (result *StatCache) {
	reqInfo := logger.GetReqInfo(ctx)
	reqID := "none"
	if reqInfo.RequestID != "" {
		reqID = reqInfo.RequestID
	}
	c.init()
	c.RLock()
	if _, ok := c.cache[reqID]; !ok {
		c.RUnlock()
		c.Lock()
		c.cache[reqID] = nil
		c.cache[reqID] = NewStatCache(c.path)
		c.Unlock()
		c.RLock()
	}
	result = c.cache[reqID]
	c.Unlock()
	return result
}

// Delete deletes a StatCache
func (c *RequestStatCache) Delete(ctx context.Context) {
	reqInfo := logger.GetReqInfo(ctx)
	reqID := "none"
	if reqInfo.RequestID != "" {
		reqID = reqInfo.RequestID
	}
	c.init()
	c.RLock()
	if _, ok := c.cache[reqID]; !ok {
		c.RUnlock()
		c.Lock()
		c.cache[reqID] = nil
		c.Unlock()
	} else {
		c.RUnlock()
	}
	// Try and force garbage collection
	debug.FreeOSMemory()
}

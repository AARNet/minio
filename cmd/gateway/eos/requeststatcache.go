package eos

import (
	"context"
	"github.com/minio/minio/cmd/logger"
	"runtime/debug"
	"sync"
)

// RequestStatCache is used for storing a cache of FileStat's per HTTP request
type RequestStatCache struct {
	sync.Mutex
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

func (c *RequestStatCache) init() {
	if c.cache == nil {
		c.cache = make(map[string]*StatCache)
	}
}

// Get returns an already existing StatCache or creates a new one and returns it
func (c *RequestStatCache) Get(ctx context.Context) *StatCache {
	c.Lock()
	c.init()
	reqInfo := logger.GetReqInfo(ctx)
	reqID := "none"
	if reqInfo.RequestID != "" {
		reqID = reqInfo.RequestID
	}
	if _, ok := c.cache[reqID]; !ok {
		c.cache[reqID] = NewStatCache(c.path)
	}
	c.Unlock()
	return c.cache[reqID]
}

// Reset deletes and recreates a StatCache
func (c *RequestStatCache) Reset(ctx context.Context) *StatCache {
	c.Lock()
	c.init()
	reqInfo := logger.GetReqInfo(ctx)
	reqID := "none"
	if reqInfo.RequestID != "" {
		reqID = reqInfo.RequestID
	}
	if _, ok := c.cache[reqID]; !ok {
		c.cache[reqID] = nil
		c.cache[reqID] = NewStatCache(c.path)
	}
	c.Unlock()
	return c.cache[reqID]
}

// Delete deletes a StatCache
func (c *RequestStatCache) Delete(ctx context.Context) {
	c.Lock()
	c.init()
	reqInfo := logger.GetReqInfo(ctx)
	reqID := "none"
	if reqInfo.RequestID != "" {
		reqID = reqInfo.RequestID
	}
	if _, ok := c.cache[reqID]; !ok {
		c.cache[reqID] = nil
	}
	c.Unlock()
	debug.FreeOSMemory()
}

package eos

import (
	"context"
	"github.com/minio/minio/cmd/logger"
	"runtime/debug"
	"sync"
)

type RequestStatCache struct {
	sync.Mutex
	path  string
	cache map[string]*StatCache
}

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
func (c *RequestStatCache) Get(ctx context.Context) *StatCache {
	c.Lock()
	c.init()
	reqInfo := logger.GetReqInfo(ctx)
	reqId := "none"
	if reqInfo.RequestID != "" {
		reqId = reqInfo.RequestID
	}
	if _, ok := c.cache[reqId]; !ok {
		c.cache[reqId] = NewStatCache(c.path)
	}
	c.Unlock()
	return c.cache[reqId]
}

func (c *RequestStatCache) Reset(ctx context.Context) *StatCache {
	c.Lock()
	c.init()
	reqInfo := logger.GetReqInfo(ctx)
	reqId := "none"
	if reqInfo.RequestID != "" {
		reqId = reqInfo.RequestID
	}
	if _, ok := c.cache[reqId]; !ok {
		c.cache[reqId] = nil
		c.cache[reqId] = NewStatCache(c.path)
	}
	c.Unlock()
	return c.cache[reqId]
}

func (c *RequestStatCache) Delete(ctx context.Context) {
	c.Lock()
	c.init()
	reqInfo := logger.GetReqInfo(ctx)
	reqId := "none"
	if reqInfo.RequestID != "" {
		reqId = reqInfo.RequestID
	}
	if _, ok := c.cache[reqId]; !ok {
		c.cache[reqId] = nil
	}
	c.Unlock()
	debug.FreeOSMemory()
}

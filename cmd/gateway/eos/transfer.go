package eos

import (
        "hash"
        "sync"
        minio "github.com/minio/minio/cmd"
)


type Transfer struct {
        sync.RWMutex
        parts       map[int]minio.PartInfo
        partsCount  int
        size        int64
        chunkSize   int64
        firstByte   byte
        contenttype string
        stagepath   string
        md5         hash.Hash
        md5PartID   int
}

func (mp *Transfer) AddToSize(size int64) {
	mp.size += size
}

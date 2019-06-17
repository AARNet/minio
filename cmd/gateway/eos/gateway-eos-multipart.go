package eos

import (
        "hash"
        "sync"
        minio "github.com/minio/minio/cmd"
)


type eosMultiPartsType struct {
        parts       map[int]minio.PartInfo
        partsCount  int
        size        int64
        chunkSize   int64
        firstByte   byte
        contenttype string
        stagepath   string
        md5         hash.Hash
        md5PartID   int
        mutex       sync.Mutex
}

func (mp *eosMultiPartsType) AddToSize(size int64) {
	mp.size += size
}

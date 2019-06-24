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
	mp.Lock()
	mp.size += size
	mp.Unlock()
}

func (mp *Transfer) IncreasePartsCount() {
	mp.Lock()
	mp.partsCount++
	mp.Unlock()
}

func (mp *Transfer) AddPart(partid int, part minio.PartInfo) {
	mp.Lock()
	mp.parts[partid] = part
	mp.Unlock()
}

func (mp *Transfer) GetStagePath() string {
	mp.RLock()
	defer mp.RUnlock()
	return mp.stagepath
}

func (mp *Transfer) GetSize() int64 {
	mp.RLock()
	defer mp.RUnlock()
	return mp.size
}

func (mp *Transfer) GetChunkSize() int64 {
	mp.RLock()
	defer mp.RUnlock()
	return mp.chunkSize
}

func (mp *Transfer) GetFirstByte() byte {
	mp.RLock()
	defer mp.RUnlock()
	return mp.firstByte
}

func (mp *Transfer) GetContentType() string {
	mp.RLock()
	defer mp.RUnlock()
	return mp.contenttype
}

func (mp *Transfer) GetMD5() hash.Hash {
	mp.RLock()
	defer mp.RUnlock()
	return mp.md5
}

func (mp *Transfer) GetMD5PartID() int {
	mp.RLock()
	defer mp.RUnlock()
	return mp.md5PartID
}

func (mp *Transfer) GetParts() map[int]minio.PartInfo {
	mp.RLock()
	defer mp.RUnlock()
	return mp.parts
}

func (mp *Transfer) GetPartsCount() int {
	mp.RLock()
	defer mp.RUnlock()
	return mp.partsCount
}

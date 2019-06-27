/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 * Michael D'Silva
 *
 */

package eos

import (
        "hash"
        "sync"
	"encoding/hex"
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

func (mp *Transfer) AddPart(partid int, part minio.PartInfo) {
	mp.Lock()
	mp.parts[partid] = part
	mp.Unlock()
}

func (mp *Transfer) AddToSize(size int64) {
	mp.size += size
}

func (mp *Transfer) GetStagePath() string {
	mp.RLock()
	defer mp.RUnlock()
	return mp.stagepath
}

func (mp *Transfer) GetContentType() string {
	mp.RLock()
	defer mp.RUnlock()
	return mp.contenttype
}

func (mp *Transfer) GetETag() string {
	mp.RLock()
	defer mp.RUnlock()
	return hex.EncodeToString(mp.md5.Sum(nil))
}

func (mp *Transfer) GetParts() map[int]minio.PartInfo {
	// TODO: Currently causing a nil or invalid pointer panic
	mp.RLock()
	defer mp.RUnlock()
	return mp.parts
}

func (mp *Transfer) GetPartsCount() int {
	mp.RLock()
	defer mp.RUnlock()
	return mp.partsCount
}

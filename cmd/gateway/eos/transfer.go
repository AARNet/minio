/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 * Michael D'Silva
 *
 */

package eos

import (
	"encoding/hex"
	minio "github.com/minio/minio/cmd"
	"hash"
	"sync"
)

// Transfer is information about a multipart transfer
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

// AddPart adds information about a part in a multipart transfer
func (mp *Transfer) AddPart(partid int, part minio.PartInfo) {
	mp.Lock()
	mp.parts[partid] = part
	mp.Unlock()
}

// AddToSize adds size to the total size of the transfer
func (mp *Transfer) AddToSize(size int64) {
	mp.Lock()
	mp.size += size
	mp.Unlock()
}

// GetStagePath returns the configured staging path
func (mp *Transfer) GetStagePath() string {
	mp.RLock()
	defer mp.RUnlock()
	return mp.stagepath
}

// GetContentType returns the content type of the transfer
func (mp *Transfer) GetContentType() string {
	mp.RLock()
	defer mp.RUnlock()
	return mp.contenttype
}

// GetETag returns the ETag for the transfer
func (mp *Transfer) GetETag() string {
	mp.RLock()
	defer mp.RUnlock()
	return hex.EncodeToString(mp.md5.Sum(nil))
}

// GetParts returns all parts for the transfer
func (mp *Transfer) GetParts() map[int]minio.PartInfo {
	mp.RLock()
	defer mp.RUnlock()
	return mp.parts
}

// GetPartsCount returns the total number of parts in the transfer
func (mp *Transfer) GetPartsCount() int {
	mp.RLock()
	defer mp.RUnlock()
	return mp.partsCount
}

// SetFirstByte -
func (mp *Transfer) SetFirstByte(firstByte byte) {
	mp.Lock()
	defer mp.Unlock()
	mp.firstByte = firstByte
}

// SetChunkSize -
func (mp *Transfer) SetChunkSize(size int64) {
	mp.Lock()
	mp.chunkSize = size
	mp.Unlock()
}

// GetChunkSize -
func (mp *Transfer) GetChunkSize() (size int64) {
	mp.RLock()
	defer mp.RUnlock()
	return mp.chunkSize
}

// IncrementPartsCount -
func (mp *Transfer) IncrementPartsCount() {
	mp.Lock()
	mp.partsCount++
	mp.Unlock()
}

/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 *
 */

package eos

import (
	minio "github.com/minio/minio/cmd"
	"sync"
)

// TransferList is a list of Transfers with a read/write lock
type TransferList struct {
	sync.RWMutex
	transfer map[string]*Transfer
}

// NewTransferList creates a new TransferList
func NewTransferList() *TransferList {
	p := new(TransferList)
	p.transfer = make(map[string]*Transfer)
	return p
}

// AddTransfer adds a Transfer to the list
func (p *TransferList) AddTransfer(id string, t *Transfer) {
	p.Lock()
	defer p.Unlock()
	p.transfer[id] = t
}

// GetTransfer returns a Transfer in the list
func (p *TransferList) GetTransfer(id string) *Transfer {
	p.RLock()
	defer p.RUnlock()
	return p.transfer[id]
}

// DeleteTransfer deletes a transfer from the list
func (p *TransferList) DeleteTransfer(id string) {
	p.RLock()
	if _, ok := p.transfer[id]; ok {
		p.RUnlock()
		p.Lock()
		delete(p.transfer, id)
		p.Unlock()
		p.RLock()
	}
	p.RUnlock()
}

// AddPartToTransfer -
func (p *TransferList) AddPartToTransfer(id string, partID int, part minio.PartInfo) {
	transfer := p.GetTransfer(id)
	if transfer != nil {
		transfer.AddPart(partID, part)
	}
}

// SetFirstByte -
func (p *TransferList) SetFirstByte(id string, firstByte byte) {
	transfer := p.GetTransfer(id)
	if transfer != nil {
		transfer.SetFirstByte(firstByte)
	}
}

// SetChunkSize -
func (p *TransferList) SetChunkSize(id string, size int64) {
	transfer := p.GetTransfer(id)
	if transfer != nil {
		transfer.SetChunkSize(size)
	}
}

// GetEtag -
func (p *TransferList) GetEtag(id string) (etag string) {
	transfer := p.GetTransfer(id)
	if transfer != nil {
		etag = transfer.GetETag()
	}
	return etag
}

// GetContentType -
func (p *TransferList) GetContentType(id string) (etag string) {
	transfer := p.GetTransfer(id)
	if transfer != nil {
		etag = transfer.GetContentType()
	}
	return etag
}

// GetChunkSize -
func (p *TransferList) GetChunkSize(id string) (size int64) {
	transfer := p.GetTransfer(id)
	if transfer != nil {
		size = transfer.GetChunkSize()
	}
	return size
}

// GetStagePath -
func (p *TransferList) GetStagePath(id string) (stage string) {
	transfer := p.GetTransfer(id)
	if transfer != nil {
		stage = transfer.GetStagePath()
	}
	return stage
}

// GetParts -
func (p *TransferList) GetParts(id string) map[int]minio.PartInfo {
	transfer := p.GetTransfer(id)
	if transfer != nil {
		return transfer.GetParts()
	}
	return nil
}

// GetPartsCount -
func (p *TransferList) GetPartsCount(id string) int {
	transfer := p.GetTransfer(id)
	if transfer != nil {
		return transfer.GetPartsCount()
	}
	return 0
}

// IncrementPartsCount -
func (p *TransferList) IncrementPartsCount(id string) {
	transfer := p.GetTransfer(id)
	if transfer != nil {
		transfer.IncrementPartsCount()
	}
}

// AddToSize -
func (p *TransferList) AddToSize(id string, size int64) {
	transfer := p.GetTransfer(id)
	if transfer != nil {
		transfer.AddToSize(size)
	}
}

// GetSize -
func (p *TransferList) GetSize(id string) int64 {
	transfer := p.GetTransfer(id)
	if transfer != nil {
		return transfer.GetSize()
	}
	return 0
}

// TransferExists checks to see if the transfer exists in the list
func (p *TransferList) TransferExists(id string) bool {
	p.RLock()
	defer p.RUnlock()
	if _, ok := p.transfer[id]; ok {
		return true
	}
	return false
}

// WaitForTransfer ...
func (p *TransferList) WaitForTransfer(uploadID string) {
	for { // TODO: Fix this infinite loop.
		parts := p.GetParts(uploadID)
		if parts != nil {
			break
		}
		Sleep()
	}
}

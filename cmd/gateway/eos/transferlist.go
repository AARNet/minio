/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 *
 */

package eos

import "sync"

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

// TransferExists checks to see if the transfer exists in the list
func (p *TransferList) TransferExists(id string) bool {
	return p.transfer[id] != nil
}

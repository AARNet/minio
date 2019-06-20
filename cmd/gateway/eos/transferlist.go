package eos

import "sync"

type TransferList struct {
	sync.RWMutex
	transfer map[string]*Transfer
}

func (p TransferList) AddTransfer(id string, t *Transfer) {
	p.transfer[id] = t
}

func (p TransferList) GetTransfer(id string) *Transfer {
	p.RLock()
	defer p.RUnlock()
	return p.transfer[id]
}

func (p *TransferList) DeleteTransfer(id string) {
	delete(p.transfer, id)
}

func (p *TransferList) TransferExists(id string) bool {
	return p.transfer[id] != nil
}

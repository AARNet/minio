package eos

type TransferList struct {
	transfer map[string]*Transfer
}

func (p *TransferList) LockTransfer(id string) {
	p.transfer[id].Lock()
}

func (p *TransferList) RLockTransfer(id string) {
	p.transfer[id].RLock()
}

func (p *TransferList) UnlockTransfer(id string) {
	p.transfer[id].Unlock()
}

func (p *TransferList) RUnlockTransfer(id string) {
	p.transfer[id].RUnlock()
}

func (p *TransferList) DeleteTransfer(id string) {
	delete(p.transfer, id)
}

func (p *TransferList) TransferExists(id string) bool {
	return p.transfer[id] != nil
}

/*
 * AARNet 2018
 *
 * Michael D'Silva
 *
 * This is a gateway for AARNet's CERN's EOS storage backend (v4.2.29)
 *
 */

package eos

import (
	"syscall"
	"time"
)

type eosFileStat struct {
	id          int64
	name        string
	size        int64
	file        bool
	modTime     time.Time
	sys         syscall.Stat_t
	etag        string
	contenttype string
}

func (fs *eosFileStat) Id() int64          { return fs.id }
func (fs *eosFileStat) Name() string       { return fs.name }
func (fs *eosFileStat) Size() int64        { return fs.size }
func (fs *eosFileStat) ModTime() time.Time { return fs.modTime }
func (fs *eosFileStat) Sys() interface{}   { return &fs.sys }
func (fs *eosFileStat) IsDir() bool        { return !fs.file }

func (fs *eosFileStat) ETag() string {
	if fs.IsDir() || fs.etag == "" {
		return "00000000000000000000000000000000"
	}
	return fs.etag
}

func (fs *eosFileStat) ContentType() string {
	if fs.IsDir() {
		return "application/x-directory"
	}
	if fs.contenttype == "" {
		return "application/octet-stream"
	}
	return fs.contenttype
}

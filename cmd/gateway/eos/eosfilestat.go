/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 * Michael D'Silva
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

const (
	defaultETag string = "00000000000000000000000000000000"
)

func (fs *eosFileStat) Id() int64 {
	return fs.id
}

func (fs *eosFileStat) Name() string {
	return fs.name
}

func (fs *eosFileStat) Size() int64 {
	return fs.size
}

func (fs *eosFileStat) ModTime() time.Time {
	return fs.modTime
}

func (fs *eosFileStat) Sys() interface{} {
	return &fs.sys
}

func (fs *eosFileStat) IsDir() bool {
	return !fs.file
}

func (fs *eosFileStat) ETag() string {
	if fs.IsDir() || fs.etag == "" {
		return defaultETag
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

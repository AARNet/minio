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

// FileStat is the stat information for an object
type FileStat struct {
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

// ID returns the FileStat ID
func (fs *FileStat) ID() int64 {
	return fs.id
}

// Name returns the file name (no path)
func (fs *FileStat) Name() string {
	return fs.name
}

// Size returns the size of the file
func (fs *FileStat) Size() int64 {
	return fs.size
}

// ModTime returns the file's modified time
func (fs *FileStat) ModTime() time.Time {
	return fs.modTime
}

// Sys returns ... TODO: find this out
func (fs *FileStat) Sys() interface{} {
	return &fs.sys
}

// IsDir returns if the file is a directory
func (fs *FileStat) IsDir() bool {
	return !fs.file
}

// ETag returns the file's etag
func (fs *FileStat) ETag() string {
	if fs.IsDir() || fs.etag == "" {
		return defaultETag
	}
	return fs.etag
}

// ContentType returns the files content type
func (fs *FileStat) ContentType() string {
	if fs.IsDir() {
		return "application/x-directory"
	}
	if fs.contenttype == "" {
		return "application/octet-stream"
	}
	return fs.contenttype
}

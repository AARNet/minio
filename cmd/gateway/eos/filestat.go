/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 * Michael D'Silva
 *
 */

package eos

import (
	"path/filepath"
	"strings"
	"time"
)

// FileStat is the stat information for an object
type FileStat struct {
	name        string
	fullpath    string
	size        int64
	file        bool
	modTime     time.Time
	etag        string
	contenttype string
}

const (
	defaultETag string = "00000000000000000000000000000000"
)

// NewFileStat creates a new *FileStat
func NewFileStat(file string, size int64, isFile bool, modTime int64, eTag string, contentType string) *FileStat {
	if eTag == "" {
		eTag = defaultETag
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	f := new(FileStat)
	f.name = strings.TrimSuffix(filepath.Base(file), "/")
	f.fullpath = file
	f.file = isFile
	f.size = size
	f.modTime = time.Unix(modTime, 0)
	f.etag = eTag
	f.contenttype = contentType
	return f
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

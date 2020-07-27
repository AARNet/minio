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
	Name        string
	FullPath    string
	Size        int64
	File        bool
	ModTime     time.Time
	Etag        string
	ContentType string
}

const (
	defaultETag  string = "00000000000000000000000000000000"
	zerobyteETag string = "d41d8cd98f00b204e9800998ecf8427e"
)

// NewFileStat creates a new *FileStat
func NewFileStat(file string, size int64, isFile bool, modTime int64, eTag string, contentType string) *FileStat {
	if eTag == "" {
		eTag = defaultETag
	}
	if size == 0 && eTag != zerobyteETag {
		eTag = zerobyteETag
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	f := new(FileStat)
	f.Name = strings.TrimSuffix(filepath.Base(file), "/")
	f.FullPath = file
	f.File = isFile
	f.Size = size
	f.ModTime = time.Unix(modTime, 0)
	f.Etag = eTag
	f.ContentType = contentType
	return f
}

// IsDir returns if the file is a directory
func (fs *FileStat) IsDir() bool {
	return !fs.File
}

// GetETag returns the file's etag
func (fs *FileStat) GetETag() string {
	if fs.IsDir() || fs.Etag == "" {
		return defaultETag
	}
	return fs.Etag
}

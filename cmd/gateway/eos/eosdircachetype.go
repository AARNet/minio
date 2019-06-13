package eos

type eosDirCacheType struct {
	objects []string
	path    string
}

func NewDirCache() eosDirCacheType {
	return eosDirCacheType{}
}

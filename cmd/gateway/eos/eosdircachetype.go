/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 * Michael D'Silva
 *
 */

package eos

type eosDirCacheType struct {
	objects []string
}

func NewDirCache() eosDirCacheType {
	return eosDirCacheType{}
}

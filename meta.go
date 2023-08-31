package main

import (
	"encoding/binary"
)

const (
	metaPageNum = 0
)


type meta struct {
	freelistPage pagenum
}

func newEmptyMeta() *meta {
	return &meta{}
}


func (m *meta) serialize(buf []byte){
	pos:=0;

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.freelistPage))
	pos += pageNumSize
}

func (m *meta) deserialize( buf []byte){
	pos :=0
	m.freelistPage = pagenum(binary.LittleEndian.Uint64(buf[pos:]))
	pos+=pageNumSize
}







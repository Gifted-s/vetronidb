package main

import (
	"encoding/binary"
)

type freelist struct {
	maxPage       pagenum   // max page allocated maxPage * pageSize = filesize
	releasedPages []pagenum // pages previously allocated but now free
}

var initialPage pagenum = 0

func newFreelist() *freelist {
	return &freelist{
		maxPage:       initialPage,
		releasedPages: []pagenum{},
	}
}

func (fr *freelist) serialize(buf []byte) []byte {
	pos := 0
	// 2 byte for storing freelist max page
	binary.LittleEndian.PutUint16(buf[pos:], uint16(fr.maxPage))
	pos += 2
	// 2 byte for storing released page count
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(fr.releasedPages)))
	pos += 2
	for _, page := range fr.releasedPages {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(page))
		pos += pageNumSize
	}
	return buf

}

func (fr *freelist) deserialize(buf []byte) {
	pos := 0
	fr.maxPage = pagenum(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	releasedPageCount := int(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	for i := 0; i < releasedPageCount; i++ {
		fr.releasedPages = append(fr.releasedPages, pagenum(binary.LittleEndian.Uint64(buf[pos:])))
		pos += pageNumSize
	}
}

func (fr *freelist) getNextPage() pagenum {
	if len(fr.releasedPages) != 0 {
		pageId := fr.releasedPages[len(fr.releasedPages)-1]
		fr.releasedPages = fr.releasedPages[:len(fr.releasedPages)-1]
		return pageId
	}

	fr.maxPage += 1
	return fr.maxPage
}

func (fr *freelist) releasedPage(page pagenum) {
	fr.releasedPages = append(fr.releasedPages, page)
}

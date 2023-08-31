package main

import (
	"fmt"
	"os"
)

type pagenum uint64

type page struct {
	num  pagenum
	data []byte
}
type dal struct {
	file     *os.File
	pageSize int
	*meta
	*freelist
}

func newDal(path string, pageSize int) (*dal, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	dal := &dal{
		file,
		pageSize,
		nil,
        newFreelist(),
	}
	return dal, nil
}

func (d *dal) close() error {

	if d.file != nil {
		err := d.file.Close()
		if err != nil {
			return fmt.Errorf("could not close file: %s", err)
		}
		d.file = nil
	}
	return nil
}

func (d *dal) allocateEmptyPage() *page {
	return &page{
		data: make([]byte, d.pageSize),
	}
}

func(d *dal) writeMeta(m *meta) (*page, error) {
	p := d.allocateEmptyPage()
	p.num = metaPageNum
	m.serialize(p.data)
	err:=d.writePage(p)
	if err != nil {
		return nil, err
	}
	return p, nil
}


func(d *dal) readMeta() (*meta, error) {
	p, err := d.readPage(metaPageNum)
	if err != nil {
		return nil, err
	}

	meta := newEmptyMeta()
	meta.deserialize(p.data);
	return meta, nil
}


func (d *dal) writeFreeList()(*page, error) {
	p:= d.allocateEmptyPage()
	p.num = d.freelistPage
	d.freelist.serialize(p.data)
	err := d.writePage(p)
	if err != nil {
		return nil, err
	}
	d.freelistPage = p.num
	return p, nil
}



func (d *dal) readPage(pageNum pagenum) (*page, error) {
	p := d.allocateEmptyPage()

	// calculate offset
	offset := int(pageNum) * d.pageSize

	// Read data at offset
	_, err := d.file.ReadAt(p.data, int64(offset))
	if err != nil {
		return nil, err
	}

	return p, nil

}

func (d *dal) writePage(p *page) error {
	// calculate correct offset
	offset := int64(p.num) * int64(d.pageSize)
	_, err := d.file.WriteAt(p.data, offset)
	return err
}


package pager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	config "github.com/brown-csci1270/db/pkg/config"
	list "github.com/brown-csci1270/db/pkg/list"

	directio "github.com/ncw/directio"
)

// Page size - defaults to 4kb.
const PAGESIZE = int64(directio.BlockSize)

// Number of pages.
const NUMPAGES = config.NumPages

// Pagers manage pages of data read from a file.
type Pager struct {
	file         *os.File             // File descriptor.
	nPages       int64                // The number of pages used by this database.
	ptMtx        sync.Mutex           // Page table mutex.
	freeList     *list.List           // Free page list.
	unpinnedList *list.List           // Unpinned page list.
	pinnedList   *list.List           // Pinned page list.
	pageTable    map[int64]*list.Link // Page table.
}

// Construct a new Pager.
func NewPager() *Pager {
	var pager *Pager = &Pager{}
	pager.pageTable = make(map[int64]*list.Link)
	pager.freeList = list.NewList()
	pager.unpinnedList = list.NewList()
	pager.pinnedList = list.NewList()
	frames := directio.AlignedBlock(int(PAGESIZE * NUMPAGES))
	for i := 0; i < NUMPAGES; i++ {
		frame := frames[i*int(PAGESIZE) : (i+1)*int(PAGESIZE)]
		page := Page{
			pager:    pager,
			pagenum:  NOPAGE,
			pinCount: 0,
			dirty:    false,
			data:     &frame,
		}
		pager.freeList.PushTail(&page)
	}
	return pager
}

// HasFile checks if the pager is backed by disk.
func (pager *Pager) HasFile() bool {
	return pager.file != nil
}

// GetFileName returns the file name.
func (pager *Pager) GetFileName() string {
	return pager.file.Name()
}

// GetNumPages returns the number of pages.
func (pager *Pager) GetNumPages() int64 {
	return pager.nPages
}

// GetFreePN returns the next available page number.
func (pager *Pager) GetFreePN() int64 {
	// Assign the first page number beyond the end of the file.
	return pager.nPages
}

// Open initializes our page with a given database file.
func (pager *Pager) Open(filename string) (err error) {
	// Create the necessary prerequisite directories.
	if idx := strings.LastIndex(filename, "/"); idx != -1 {
		err = os.MkdirAll(filename[:idx], 0775)
		if err != nil {
			return err
		}
	}
	// Open or create the db file.
	pager.file, err = directio.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	// Get info about the size of the pager.
	var info os.FileInfo
	var len int64
	if info, err = pager.file.Stat(); err == nil {
		len = info.Size()
		if len%PAGESIZE != 0 {
			return errors.New("open: DB file has been corrupted")
		}
	}
	// Set the number of pages and hand off initialization to someone else.
	pager.nPages = len / PAGESIZE
	return nil
}

// Close signals our pager to flush all dirty pages to disk.
func (pager *Pager) Close() (err error) {
	// Prevent new data from being paged in.
	pager.ptMtx.Lock()
	// Check if all refcounts are 0.
	curLink := pager.pinnedList.PeekHead()
	if curLink != nil {
		fmt.Println("ERROR: pages are still pinned on close")
	}
	// Cleanup.
	pager.FlushAllPages()
	if pager.file != nil {
		err = pager.file.Close()
	}
	pager.ptMtx.Unlock()
	return err
}

// Populate a page's data field, given a pagenumber.
func (pager *Pager) ReadPageFromDisk(page *Page, pagenum int64) error {
	if _, err := pager.file.Seek(pagenum*PAGESIZE, 0); err != nil {
		return err
	}
	if _, err := pager.file.Read(*page.data); err != nil && err != io.EOF {
		return err
	}
	return nil
}

// NewPage returns an unused buffer from the free or unpinned list
// the ptMtx should be locked on entry
func (pager *Pager) NewPage(pagenum int64) (*Page, error) {
	// 如果freelist有空
	cur := pager.freeList.PeekHead()
	if cur != nil{
		// get current page
		cur_page := cur.GetKey().(*Page)
		// pop the page from freelist
		pager.freeList.PeekHead().PopSelf()
		// add pinCount
		cur_page.pinCount = 1
		// update pagenum
		cur_page.pagenum = pagenum
		// // init amount of page
		pager.nPages += 1
		// update pagetable
		pager.pageTable[pagenum] = cur
		// return
		return cur_page, nil
	}else{
		// 如果unpinnedlist有空
		cur_unpin := pager.unpinnedList.PeekHead()
		if cur_unpin != nil{
			// get current page
			cur_unpin_page := cur_unpin.GetKey().(*Page)
			// pop the page from unpinned list
			pager.unpinnedList.PeekHead().PopSelf()
			// add pinCount
			cur_unpin_page.pinCount = 1
			// update pagenum
			cur_unpin_page.pagenum = pagenum
			// // init amount of page
			pager.nPages += 1
			// update pagetable TODO update it in getpage
			pager.pageTable[pagenum] = cur_unpin
			return cur_unpin_page, nil
		}else{
			return nil, errors.New("NewPage: only pinned page is available")
		}
	}
}

// getPage returns the page corresponding to the given pagenum.
func (pager *Pager) GetPage(pagenum int64) (page *Page, err error) {
	// check invalid
	if pagenum>NUMPAGES {
		return nil, errors.New("GetPage: invalid pagenum > NUMPAGES")
	}
	// TODO less than zero check
	if pagenum<0 {
		return nil, errors.New("GetPage: invalid pagenum < 0")
	}

	// if the page in the map
	if page, ok := pager.pageTable[pagenum]; ok {
		// get the current page
		cur_page := page.GetKey().(*Page)
		// check whether the current page comes from the unpinned list
		found := pager.unpinnedList.Find(func(l *list.Link) bool { 
			return l.GetKey() == pagenum
		})
		// if it is from the unpinned page
		if found != nil{
			// pop the page from unpinned list
			page.PopSelf()
			// double check if the pinCount is zero, it means really unpinned, then push it to pinned list
			if cur_page.pinCount == 0{
				fmt.Println("f*ck")
				cur_page.pinCount += 1
				pager.pinnedList.PushTail(&cur_page)
			}
			// cur_page.pinCount += 1 
		}

		// TODO check valid read, if not, put current page to freelist
		data_check := pager.ReadPageFromDisk(cur_page, pagenum)
		if data_check == nil{
			pager.freeList.PushTail(&cur_page)
			cur_page.pinCount = 0
			return nil, errors.New("GetPage: the data in the page is not valid")
		}
		return cur_page, nil
	}else{
		new_page, _ := pager.NewPage(pagenum)
		// TODO add amount page pinned list
		if new_page != nil{
			// new_page.dirty = true
			pager.pinnedList.PushTail(&new_page)
			return new_page, nil
		}
	}
	return nil, nil
}

// Flush a particular page to disk.
func (pager *Pager) FlushPage(page *Page) {
	pagenum := page.pagenum
	is_dirty := page.dirty
	cur, ok := pager.pageTable[pagenum]
	// when page is both dirty and exists
	if ok && is_dirty {
		// get the current page
		cur_page := cur.GetKey().(*Page)
		// get the page data
		page_data := cur_page.data
		// write data to disk
		pager.file.WriteAt(*page_data, pagenum*int64(PAGESIZE))
		// empty pin count
		// page.pinCount = 0
		// update dirty
		page.dirty = false
		// pop the current page whenever it is
		// cur.PopSelf()
		// test: try to minus nPages
		// pager.nPages += 1
		// push it into free list
		// pager.freeList.PushTail(&cur_page)
		
	}
}

// Flushes all dirty pages.
func (pager *Pager) FlushAllPages() {
	pin_l := pager.pinnedList
	pin_l.Map(func(l *list.Link) {
		cur_page := l.GetKey().(*Page)
		pager.FlushPage(cur_page)
	})
	unpin_l := pager.unpinnedList
	unpin_l.Map(func(l *list.Link) {
		cur_page := l.GetKey().(*Page)
		pager.FlushPage(cur_page)
	})
}

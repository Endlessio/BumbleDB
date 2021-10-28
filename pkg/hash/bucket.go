package hash

import (
	"errors"
	"fmt"
	"io"

	pager "github.com/brown-csci1270/db/pkg/pager"
	utils "github.com/brown-csci1270/db/pkg/utils"
)

// HashBucket.
type HashBucket struct {
	depth   int64
	numKeys int64
	page    *pager.Page
}

// Construct a new HashBucket.
func NewHashBucket(pager *pager.Pager, depth int64) (*HashBucket, error) {
	newPN := pager.GetFreePN()
	newPage, err := pager.GetPage(newPN)
	if err != nil {
		return nil, err
	}
	bucket := &HashBucket{depth: depth, numKeys: 0, page: newPage}
	bucket.updateDepth(depth)
	return bucket, nil
}

// Get local depth.
func (bucket *HashBucket) GetDepth() int64 {
	return bucket.depth
}

// Get a bucket's page.
func (bucket *HashBucket) GetPage() *pager.Page {
	return bucket.page
}

// Finds the entry with the given key.
func (bucket *HashBucket) Find(key int64) (utils.Entry, bool) {
	for i:= 0; i < int(bucket.numKeys); i++ {
		cur_entry := bucket.getCell(int64(i))
		if key == cur_entry.GetKey() {
			return cur_entry, true
		}
	}
	return nil, false
}


// Inserts the given key-value pair, splits if necessary.
func (bucket *HashBucket) Insert(key int64, value int64) (bool, error) {
	// no enough space
	if bucket.numKeys == BUCKETSIZE {
		bucket.modifyCell(bucket.numKeys, HashEntry{key, value})
		// bucket.updateDepth(bucket.GetDepth()+1)
		bucket.updateNumKeys(0)
		return true, nil
	} else if bucket.numKeys < BUCKETSIZE {
		bucket.modifyCell(bucket.numKeys, HashEntry{key, value})
		bucket.updateNumKeys(bucket.numKeys+1)
		return false, nil
	} else {
		return false, errors.New("bucket/insert: the depth is larger than BUCKETSIZE")
	}
}

// Update the given key-value pair, should never split.
func (bucket *HashBucket) Update(key int64, value int64) error {
	flag := false
	for i:= int64(0); i < bucket.numKeys; i++ {
		cur_key := bucket.getKeyAt(i)
		if key == cur_key {
			bucket.updateValueAt(i, value)
			flag = true
		}
	}
	if !flag {
		return errors.New("bucket/update: the key is not find in current bucket")
	}
	return nil
}

// Delete the given key-value pair, does not coalesce.
func (bucket *HashBucket) Delete(key int64) error {
	target_idx := int64(-1)
	// find the index of the target key
	for i := int64(0); i < bucket.numKeys; i++ {
		cur_key := bucket.getKeyAt(i)
		if cur_key == key {
			target_idx = i
			break
		}
	}
	// if find
	if target_idx != int64(-1) {
		// shift the right cells one step forward
		for j := target_idx; j < bucket.numKeys-1; j++ {
			next_key := bucket.getKeyAt(j+1)
			next_val := bucket.getValueAt(j+1)
			bucket.updateKeyAt(j, next_key)
			bucket.updateValueAt(j, next_val)
		}
		// update the key number
		bucket.updateNumKeys(bucket.numKeys-1)
		return nil
	// if not find
	} else {
		return errors.New("bucket/delete: the target delete key is not found in current bucket")
	}
}

// Select all entries in this bucket.
// TODO: when should return error
func (bucket *HashBucket) Select() ([]utils.Entry, error) {
	var res []utils.Entry
	for i := int64(0); i < bucket.numKeys; i++ {
		cur_entry := bucket.getCell(i)
		res = append(res, cur_entry)
	}
	return res, nil
}

// Pretty-print this bucket.
func (bucket *HashBucket) Print(w io.Writer) {
	io.WriteString(w, fmt.Sprintf("bucket depth: %d\n", bucket.depth))
	io.WriteString(w, "entries:")
	for i := int64(0); i < bucket.numKeys; i++ {
		bucket.getCell(i).Print(w)
	}
	io.WriteString(w, "\n")
}

// [CONCURRENCY] Grab a write lock on the hash table index
func (bucket *HashBucket) WLock() {
	bucket.page.WLock()
}

// [CONCURRENCY] Release a write lock on the hash table index
func (bucket *HashBucket) WUnlock() {
	bucket.page.WUnlock()
}

// [CONCURRENCY] Grab a read lock on the hash table index
func (bucket *HashBucket) RLock() {
	bucket.page.RLock()
}

// [CONCURRENCY] Release a read lock on the hash table index
func (bucket *HashBucket) RUnlock() {
	bucket.page.RUnlock()
}

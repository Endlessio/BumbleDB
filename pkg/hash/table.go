package hash

import (
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"

	pager "github.com/brown-csci1270/db/pkg/pager"
	utils "github.com/brown-csci1270/db/pkg/utils"
)

// HashTable definitions.
type HashTable struct {
	depth   int64
	buckets []int64 // Array of bucket page numbers
	pager   *pager.Pager
	rwlock  sync.RWMutex // Lock on the hash table index
}

// Returns a new HashTable.
func NewHashTable(pager *pager.Pager) (*HashTable, error) {
	depth := int64(2)
	buckets := make([]int64, powInt(2, depth))
	for i := range buckets {
		bucket, err := NewHashBucket(pager, depth)
		if err != nil {
			return nil, err
		}
		buckets[i] = bucket.page.GetPageNum()
		bucket.page.Put()
	}
	return &HashTable{depth: depth, buckets: buckets, pager: pager}, nil
}

// [CONCURRENCY] Grab a write lock on the hash table index
func (table *HashTable) WLock() {
	table.rwlock.Lock()
}

// [CONCURRENCY] Release a write lock on the hash table index
func (table *HashTable) WUnlock() {
	table.rwlock.Unlock()
}

// [CONCURRENCY] Grab a read lock on the hash table index
func (table *HashTable) RLock() {
	table.rwlock.RLock()
}

// [CONCURRENCY] Release a read lock on the hash table index
func (table *HashTable) RUnlock() {
	table.rwlock.RUnlock()
}

// Get depth.
func (table *HashTable) GetDepth() int64 {
	return table.depth
}

// Get bucket page numbers.
func (table *HashTable) GetBuckets() []int64 {
	return table.buckets
}

// Get pager.
func (table *HashTable) GetPager() *pager.Pager {
	return table.pager
}

// Finds the entry with the given key.
func (table *HashTable) Find(key int64) (utils.Entry, error) {
	hashed_key := Hasher(key, table.GetDepth())
	cur_bucket, ok := table.GetBucket(hashed_key)
	if ok != nil {
		return nil, errors.New("table/find: cannot find the corresponding bucket")
	} else {
		entry, exist := cur_bucket.Find(key)
		if exist {
			return entry, nil
		} else {
			return nil, errors.New("table/find: cannot find the corresponding entry")
		}
	}
}

// ExtendTable increases the global depth of the table by 1.
func (table *HashTable) ExtendTable() {
	table.depth = table.depth + 1
	table.buckets = append(table.buckets, table.buckets...)
}


// Split the given bucket into two, extending the table if necessary.
func (table *HashTable) Split(bucket *HashBucket, hash int64) error {
	odd_local_depth := bucket.GetDepth()
	new_local_depth := odd_local_depth+1
	PN := bucket.page.GetPager()

	odd_bucket_64 := hash
	new_bucket_64 := int64(len(table.GetBuckets()))+odd_bucket_64

	// update local depth
	bucket.updateDepth(new_local_depth)

	// generate new buckets
	new_bucket, err_new := NewHashBucket(PN, bucket.GetDepth())
	if err_new != nil {
		return errors.New("bucket/split: cannot generate new bucket")
	}

	// put values into the correct bucket
	for i:=int64(0); i<bucket.numKeys; i++ {
		cur_key := bucket.getKeyAt(i)
		cur_val := bucket.getValueAt(i)
		key_hash := Hasher(cur_key, bucket.GetDepth())
		if key_hash == new_bucket_64 {
			// don't worry about bad hash for now
			_, ist_err := new_bucket.Insert(cur_key, cur_val)
			if ist_err != nil {
				return errors.New("bucket/split: cannot insert into new bucket")
			}
		} else if key_hash == odd_bucket_64 {
			bucket.modifyCell(bucket.numKeys, HashEntry{cur_key, cur_val})
			bucket.updateNumKeys(bucket.numKeys+1)
		}
	}
	//check if local depth larger than global depth
	if new_local_depth > table.GetDepth() {
		table.ExtendTable()
		// reassign the buckets
		buckets := table.GetBuckets()
		buckets[new_bucket_64] = new_bucket.page.GetPageNum()
	}
	return nil
}

// Inserts the given key-value pair, splits if necessary.
func (table *HashTable) Insert(key int64, value int64) error {
	hashed_key := Hasher(key, table.GetDepth())
	cur_bucket, ok := table.GetBucket(hashed_key)
	if ok != nil {
		return errors.New("table/insert: cannot find the bucket")
	} else {
		split, err := cur_bucket.Insert(key, value)
		if err != nil {
			return errors.New("table/insert: cannot insert")
		} 
		if split {
			split_err := table.Split(cur_bucket, hashed_key)
			if split_err != nil {
				return errors.New("table/insert: cannot split")
			} else {
				return nil
			}
		} else {
			return nil
		}
	}
}

// Update the given key-value pair.
func (table *HashTable) Update(key int64, value int64) error {
	hashed_key := Hasher(key, table.GetDepth())
	cur_bucket, ok := table.GetBucket(hashed_key)
	if ok != nil {
		return errors.New("table/update: cannot find the bucket")
	} else {
		err := cur_bucket.Update(key, value)
		if err != nil {
			return errors.New("table/update: cannot update")
		} else {
			return nil
		}
	}
}

// Delete the given key-value pair, does not coalesce.
func (table *HashTable) Delete(key int64) error {
	hashed_key := Hasher(key, table.GetDepth())
	cur_bucket, ok := table.GetBucket(hashed_key)
	if ok != nil {
		return errors.New("table/delete: cannot find the bucket")
	} else {
		err := cur_bucket.Delete(key)
		if err != nil {
			return errors.New("table/delete: cannot delete, key not exist")
		} else {
			return nil
		}
	}
}

// Select all entries in this table.
func (table *HashTable) Select() ([]utils.Entry, error) {
	var res []utils.Entry
	entry_arr := table.GetBuckets()
	for i := 0; i < len(entry_arr); i++ {
		cur_bucket, err := table.GetBucketByPN(entry_arr[i])
		if err != nil {
			return nil, errors.New("table/select: cannot get the bucket with current PN")
		} else {
			cur_entry, ok := cur_bucket.Select()
			if ok != nil {
				return nil, errors.New("table/select: current bucket select err")
			} else {
				res = append(res, cur_entry...)
			}
		}
	}
	return res, nil
}

// Print out each bucket.
func (table *HashTable) Print(w io.Writer) {
	table.RLock()
	defer table.RUnlock()
	io.WriteString(w, "====\n")
	io.WriteString(w, fmt.Sprintf("global depth: %d\n", table.depth))
	for i := range table.buckets {
		io.WriteString(w, fmt.Sprintf("====\nbucket %d\n", i))
		bucket, err := table.GetBucket(int64(i))
		if err != nil {
			continue
		}
		bucket.RLock()
		bucket.Print(w)
		bucket.RUnlock()
		bucket.page.Put()
	}
	io.WriteString(w, "====\n")
}

// Print out a specific bucket.
func (table *HashTable) PrintPN(pn int, w io.Writer) {
	table.RLock()
	defer table.RUnlock()
	if int64(pn) >= table.pager.GetNumPages() {
		fmt.Println("out of bounds")
		return
	}
	bucket, err := table.GetBucketByPN(int64(pn))
	if err != nil {
		return
	}
	bucket.RLock()
	bucket.Print(w)
	bucket.RUnlock()
	bucket.page.Put()
}

// x^y
func powInt(x, y int64) int64 {
	return int64(math.Pow(float64(x), float64(y)))
}

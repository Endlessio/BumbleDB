package hash

import (
	"errors"
	"fmt"
	"io"
	"math"
	// "strconv"
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
	// hash := ^(0xFFFFFFFF << table.GetDepth()) & hashed_key
	cur_bucket, ok := table.GetBucket(hashed_key)
	if ok != nil {
		return nil, errors.New("table/find: cannot find the corresponding bucket")
	} 
	defer cur_bucket.page.Put()
	entry, exist := cur_bucket.Find(key)
	if exist {
		return entry, nil
	} else {
		fmt.Println("table/find: cannot find current key", key, hashed_key, table.depth)
		return nil, errors.New("table/find: cannot find the corresponding entry")
	}
	
}

// ExtendTable increases the global depth of the table by 1.
func (table *HashTable) ExtendTable() {
	table.depth = table.depth + 1
	table.buckets = append(table.buckets, table.buckets...)
}



// Split the given bucket into two, extending the table if necessary.
func (table *HashTable) Split(bucket *HashBucket, hash int64) error {
	// fmt.Println("table/split")
	old_local_depth := bucket.GetDepth()
	new_local_depth := old_local_depth+1

	old_bucket_64 := ^(0xFFFFFFFF << old_local_depth) & hash
	// old_bucket_64 := Hasher(hash, old_local_depth)
	var new_bucket_64 int64

	// get the new bucket index
	if new_local_depth <= table.GetDepth() {
		// fmt.Println("not extend", odd_local_depth, new_local_depth, table.GetDepth())
		new_bucket_64 = int64(math.Pow(2, float64(old_local_depth)))+old_bucket_64
	} else {
		// fmt.Println("extend", odd_local_depth, new_local_depth, table.GetDepth())
		new_bucket_64 = int64(math.Pow(2, float64(table.depth)))+old_bucket_64
	}
	// fmt.Println("odd_bucket_64, new_bucket_64", odd_bucket_64, new_bucket_64)

	// update local depth
	bucket.updateDepth(new_local_depth)

	// generate new buckets
	new_bucket, err_new := NewHashBucket(table.pager, new_local_depth)
	if err_new != nil {
		// fmt.Println("fff")
		return errors.New("table/split: cannot generate new bucket")
	}
	defer new_bucket.page.Put()

	// fmt.Println("table/split: start reassign bucket")
	buckets := table.GetBuckets()
	//check if local depth larger than global depth
	if new_local_depth > table.GetDepth() {
		// fmt.Println("table/split: local>global")
		table.ExtendTable()
		// reassign the buckets
		// fmt.Println("table/split: new bucket", new_bucket_64, len(table.buckets), BUCKETSIZE)
		table.buckets[new_bucket_64] = new_bucket.page.GetPageNum()
		// defer new_bucket.page.Put()
	} else {
		// fmt.Println("table/split: local<global")
		for i:=int64(0); i<int64(len(buckets)); i++ {
			if buckets[i] == bucket.page.GetPageNum() {
				bin_table := ^(0xFFFFFFFF << new_local_depth) & i
				if bin_table == new_bucket_64 {
					buckets[i] = new_bucket.page.GetPageNum()
				} else if bin_table == old_bucket_64 {
					buckets[i] = bucket.page.GetPageNum()
				} else {
					return errors.New("table/split: the entry cannot be assigned to either new or old bucket")
				}
			}
		}
		// defer bucket.page.Put()
	}
	// fmt.Println("table/split: done reassign bucket")

	// fmt.Println("table/split: start put value into correct bucket")
	// put values into the correct bucket
	for i:=int64(0); i<BUCKETSIZE; i++ {
		cur_key := bucket.getKeyAt(i)
		cur_val := bucket.getValueAt(i)
		check := Hasher(cur_key, bucket.GetDepth())
		// key_hash := Hasher(cur_key, bucket.GetDepth())
		// check := ^(0xFFFFFFFF << new_local_depth) & key_hash
		// fmt.Println("cur key, cur val", i, cur_key, cur_val, check, new_bucket_64, odd_bucket_64)
		if check == new_bucket_64 {
			// new_bucket.updateKeyAt(new_bucket.numKeys, cur_key)
			// new_bucket.updateValueAt(new_bucket.numKeys, cur_val)
			// new_bucket.updateNumKeys(new_bucket.numKeys+1)
			split, ist_err := new_bucket.Insert(cur_key, cur_val)
			// ist_err := table.Insert(cur_key, cur_val)
			if ist_err != nil {
				return errors.New("table/split: cannot insert into new bucket")
			}
			if split {
				// fmt.Println("table/split: re-split on new bucket", new_bucket_64)
				ok := table.Split(new_bucket, new_bucket_64)
				if ok != nil {
					return errors.New("table/split: recursive split err")
				}
			}
			// new_bucket.modifyCell(new_bucket.numKeys, HashEntry{cur_key, cur_val})
			// new_bucket.updateNumKeys(new_bucket.numKeys+1)
		} else if check == old_bucket_64 {
			split, ist_err := bucket.Insert(cur_key, cur_val)
			// ist_err := table.Insert(cur_key, cur_val)
			if ist_err != nil {
				return errors.New("table/split: cannot insert into new bucket")
			}
			if split {
				// fmt.Println("table/split: re-split on old bucket", old_bucket_64)
				ok := table.Split(bucket, old_bucket_64)
				if ok != nil {
					return errors.New("table/split: recursive split err")
				}
			// 	// fmt.Println("year! 2")
			}
			// bucket.updateKeyAt(bucket.numKeys, cur_key)
			// bucket.updateValueAt(bucket.numKeys, cur_val)
			// bucket.updateNumKeys(bucket.numKeys+1)
			// bucket.modifyCell(bucket.numKeys, HashEntry{cur_key, cur_val})
			// bucket.updateNumKeys(bucket.numKeys+1)
		} else {
			return errors.New("table/split: key not corresponds to old/new bucket")
		}
	}


	return nil
}

// Inserts the given key-value pair, splits if necessary.
func (table *HashTable) Insert(key int64, value int64) error {
	// fmt.Println("table/insert", key, value)
	hashed_key := Hasher(key, table.GetDepth())
	// hash := ^(0xFFFFFFFF << table.depth) & hashed_key
	fmt.Println("table/insert", key, hashed_key, table.GetDepth())
	cur_bucket, ok := table.GetBucket(hashed_key)
	if ok != nil {
		return errors.New("table/insert: cannot find the bucket")
	}
	defer cur_bucket.page.Put()

	split, err := cur_bucket.Insert(key, value)
	if err != nil {
		return errors.New("table/insert: cannot insert")
	} 
	if split {
		split_err := table.Split(cur_bucket, hashed_key)
		if split_err != nil {
			return errors.New("table/insert: cannot split")
		} else {
			ok := table.Insert(key, value)
			if ok!=nil {
				return errors.New("table/insert: after split, cannot insert last one")
			}
			return nil
		}
	} else {
		return nil
	}
}


// Update the given key-value pair.
func (table *HashTable) Update(key int64, value int64) error {
	// fmt.Println("table/update")
	hashed_key := Hasher(key, table.GetDepth())
	// hash := ^(0xFFFFFFFF << table.GetDepth()) &  hashed_key
	cur_bucket, ok := table.GetBucket(hashed_key)
	if ok != nil {
		return errors.New("table/update: cannot find the bucket")
	}
	defer cur_bucket.page.Put()
	err := cur_bucket.Update(key, value)
	if err != nil {
		return errors.New("table/update: cannot update")
	} else {
		return nil
	}
	
}

// Delete the given key-value pair, does not coalesce.
func (table *HashTable) Delete(key int64) error {
	// fmt.Println("table/delete")
	hashed_key := Hasher(key, table.GetDepth())
	// hash := ^(0xFFFFFFFF << table.GetDepth()) & hashed_key
	cur_bucket, ok := table.GetBucket(hashed_key)
	if ok != nil {
		return errors.New("table/delete: cannot find the bucket")
	} 
	defer cur_bucket.page.Put()
	err := cur_bucket.Delete(key)
	if err != nil {
		return errors.New("table/delete: cannot delete, key not exist")
	} else {
		return nil
	}
	
}

// Select all entries in this table.
func (table *HashTable) Select() ([]utils.Entry, error) {
	var res []utils.Entry
	entry_arr := table.GetBuckets()
	var cur_bucket *HashBucket
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
	defer cur_bucket.page.Put()
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

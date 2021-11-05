package query

import (
	"context"
	"errors"
	"fmt"
	"os"

	db "github.com/brown-csci1270/db/pkg/db"
	hash "github.com/brown-csci1270/db/pkg/hash"
	utils "github.com/brown-csci1270/db/pkg/utils"

	errgroup "golang.org/x/sync/errgroup"
)

var DEFAULT_FILTER_SIZE int64 = 1024

// Entry pair struct - output of a join.
type EntryPair struct {
	l utils.Entry
	r utils.Entry
}

// Int pair struct - to keep track of seen bucket pairs.
type pair struct {
	l int64
	r int64
}

// buildHashIndex constructs a temporary hash table for all the entries in the given sourceTable.
func buildHashIndex(
	sourceTable db.Index,
	useKey bool,
) (tempIndex *hash.HashIndex, dbName string, err error) {
	// Get a temporary db file.
	dbName, err = db.GetTempDB()
	if err != nil {
		return nil, "", err
	}
	// Init the temporary hash table.
	tempIndex, err = hash.OpenTable(dbName)
	if err != nil {
		return nil, "", err
	}
	// Build the hash index.
	// get start cursor
	cursor, step_err := sourceTable.TableStart()
	if step_err != nil {
		return nil, "", err
	}
	// before reaching end, do while loop by using stepForward
	for step_err == nil {
		if cursor.IsEnd() {
			step_err = cursor.StepForward()
			continue
		}
		cur_entry, err := cursor.GetEntry()
		// get the current entry
		if err != nil {
			return nil, "", err
		}
		if useKey {
			// fmt.Println("hash_join/buildHashIndex: usekey, steping forward, entry: ", cur_entry.GetKey(), cur_entry.GetValue())
			tempIndex.Insert(cur_entry.GetKey(), cur_entry.GetValue())
		} else {
			// fmt.Println("hash_join/buildHashIndex: not usekey, steping forward, entry: ", cur_entry.GetKey(), cur_entry.GetValue())
			tempIndex.Insert(cur_entry.GetValue(), cur_entry.GetKey())
		}
		// step forward
		step_err = cursor.StepForward()
		// if step_err != nil {
		// 	// fmt.Println("step_err:", step_err, cur_entry.GetKey(), cur_entry.GetValue())
		// 	break
		// }
	}
	return tempIndex, dbName, nil
}

// sendResult attempts to send a single join result to the resultsChan channel as long as the errgroup hasn't been cancelled.
func sendResult(
	ctx context.Context,
	resultsChan chan EntryPair,
	result EntryPair,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case resultsChan <- result:
		return nil
	}
}

// See which entries in rBucket have a match in lBucket.
func probeBuckets(
	ctx context.Context,
	resultsChan chan EntryPair,
	lBucket *hash.HashBucket,
	rBucket *hash.HashBucket,
	joinOnLeftKey bool,
	joinOnRightKey bool,
) error {
	defer lBucket.GetPage().Put()
	defer rBucket.GetPage().Put()
	// Probe buckets.
	fmt.Println("enter hash_join/probeBuckets")
	// get the left bucket entrys
	left_entrys, err := lBucket.Select()
	if err != nil {
		return err
	}
	
	right_entrys, err := rBucket.Select()
	if err != nil {
		return err
	}

	// create bloom filter for right bucket
	bloom_filter := CreateFilter(DEFAULT_FILTER_SIZE)

	for _, r_entry := range right_entrys {
		bloom_filter.Insert(r_entry.GetKey())
	}

	// iterate the left table
	for idx, l_entry := range left_entrys {
		var contain bool
		if joinOnLeftKey {
			contain = bloom_filter.Contains(l_entry.GetKey())
		} else {
			contain = bloom_filter.Contains(l_entry.GetValue())
		}
		
		if contain {
			fmt.Println(idx, "iteration, contain")
			// left: key, right: key
			if joinOnLeftKey && joinOnRightKey{
				// start to iterate the right bucket
				fmt.Println("hash_join/probebucket: left-key, right-key")
				for _, r_entry := range right_entrys {
					if l_entry.GetKey() == r_entry.GetKey() {
						left := hash.HashEntry{}
						left.SetKey(l_entry.GetKey())
						left.SetValue(l_entry.GetValue())
						right := hash.HashEntry{}
						right.SetKey(r_entry.GetKey())
						right.SetValue(r_entry.GetValue())
						err := sendResult(ctx, resultsChan, EntryPair{left, right})
						if err != nil {
							return err
						}
					}
				}
			// left: value, right: value
			} else if !joinOnLeftKey && !joinOnRightKey {
				fmt.Println("hash_join/probebucket: left-value, right-value")
				// start to iterate the right bucket
				for _, r_entry := range right_entrys {
					if l_entry.GetKey() == r_entry.GetKey() {
						left := hash.HashEntry{}
						left.SetKey(l_entry.GetValue())
						left.SetValue(l_entry.GetKey())
						right := hash.HashEntry{}
						right.SetKey(r_entry.GetValue())
						right.SetValue(r_entry.GetKey())
						err := sendResult(ctx, resultsChan, EntryPair{left, right})
						if err != nil {
							return err
						}
					}
				}
			// left: key, right: value
			} else if joinOnLeftKey && !joinOnRightKey{
				fmt.Println("hash_join/probebucket: left-key, right-value")
				// start to iterate the right bucket
				for _, r_entry := range right_entrys {
					if l_entry.GetKey() == r_entry.GetKey() {
						left := hash.HashEntry{}
						left.SetKey(l_entry.GetKey())
						left.SetValue(l_entry.GetValue())
						right := hash.HashEntry{}
						right.SetKey(r_entry.GetValue())
						right.SetValue(r_entry.GetKey())
						err := sendResult(ctx, resultsChan, EntryPair{left, right})
						if err != nil {
							return err
						}
					}
				}
			// left: value, right: key
			} else if !joinOnLeftKey && joinOnRightKey{
				fmt.Println("hash_join/probebucket: left-value, right-key")
				// start to iterate the right bucket
				for _, r_entry := range right_entrys {
					if l_entry.GetKey() == r_entry.GetKey() {
						left := hash.HashEntry{}
						left.SetKey(l_entry.GetValue())
						left.SetValue(l_entry.GetKey())
						right := hash.HashEntry{}
						right.SetKey(r_entry.GetKey())
						right.SetValue(r_entry.GetValue())
						err := sendResult(ctx, resultsChan, EntryPair{left, right})
						if err != nil {
							return err
						}
					}
				}
			// else invalid
			} else {
				return errors.New("hash_join/probeBuckets: join key error")
			}
		}
	}
	return nil
}

// Join leftTable on rightTable using Grace Hash Join.
func Join(
	ctx context.Context,
	leftTable db.Index,
	rightTable db.Index,
	joinOnLeftKey bool,
	joinOnRightKey bool,
) (chan EntryPair, context.Context, *errgroup.Group, func(), error) {
	leftHashIndex, leftDbName, err := buildHashIndex(leftTable, joinOnLeftKey)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	rightHashIndex, rightDbName, err := buildHashIndex(rightTable, joinOnRightKey)
	if err != nil {
		os.Remove(leftDbName)
		os.Remove(leftDbName + ".meta")
		return nil, nil, nil, nil, err
	}
	cleanupCallback := func() {
		os.Remove(leftDbName)
		os.Remove(leftDbName + ".meta")
		os.Remove(rightDbName)
		os.Remove(rightDbName + ".meta")
	}
	// Make both hash indices the same global size.
	leftHashTable := leftHashIndex.GetTable()
	rightHashTable := rightHashIndex.GetTable()
	for leftHashTable.GetDepth() != rightHashTable.GetDepth() {
		if leftHashTable.GetDepth() < rightHashTable.GetDepth() {
			// Split the left table
			leftHashTable.ExtendTable()
		} else {
			// Split the right table
			rightHashTable.ExtendTable()
		}
	}
	// Probe phase: match buckets to buckets and emit entries that match.
	group, ctx := errgroup.WithContext(ctx)
	resultsChan := make(chan EntryPair, 1024)
	// Iterate through hash buckets, keeping track of pairs we've seen before.
	leftBuckets := leftHashTable.GetBuckets()
	rightBuckets := rightHashTable.GetBuckets()
	seenList := make(map[pair]bool)
	for i, lBucketPN := range leftBuckets {
		rBucketPN := rightBuckets[i]
		bucketPair := pair{l: lBucketPN, r: rBucketPN}
		if _, seen := seenList[bucketPair]; seen {
			continue
		}
		seenList[bucketPair] = true

		lBucket, err := leftHashTable.GetBucketByPN(lBucketPN)
		if err != nil {
			return nil, nil, nil, cleanupCallback, err
		}
		rBucket, err := rightHashTable.GetBucketByPN(rBucketPN)
		if err != nil {
			lBucket.GetPage().Put()
			return nil, nil, nil, cleanupCallback, err
		}
		// l_res, _:=lBucket.Select()
		// r_res, _:=rBucket.Select()
		// fmt.Println("hash_join/Join: left, right: ", len(l_res), len(r_res))
		group.Go(func() error {
			return probeBuckets(ctx, resultsChan, lBucket, rBucket, joinOnLeftKey, joinOnRightKey)
		})
	}
	return resultsChan, ctx, group, cleanupCallback, nil
}

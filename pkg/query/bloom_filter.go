package query

import (
	bitset "github.com/bits-and-blooms/bitset"
	hash "github.com/brown-csci1270/db/pkg/hash"
)

type BloomFilter struct {
	size int64
	bits *bitset.BitSet
}

// CreateFilter initializes a BloomFilter with the given size.
func CreateFilter(size int64) *BloomFilter {
	bloom_filter := BloomFilter{size: size, bits: bitset.New(uint(size))}
	return &bloom_filter
}

// Insert adds an element into the bloom filter.
func (filter *BloomFilter) Insert(key int64) {
	hash1 := hash.MurmurHasher(key, filter.size)
	hash2 := hash.XxHasher(key, filter.size)
	filter.bits.Set(hash1)
	filter.bits.Set(hash2)
}

// Contains checks if the given key can be found in the bloom filter/
func (filter *BloomFilter) Contains(key int64) bool {
	hash1 := hash.MurmurHasher(key, filter.size)
	hash2 := hash.XxHasher(key, filter.size)
	return filter.bits.Test(hash1) && filter.bits.Test(hash2)
}

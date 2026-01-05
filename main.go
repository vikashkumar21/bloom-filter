package main

import (
	"fmt"
	"hash"

	"github.com/google/uuid"
	"github.com/spaolacci/murmur3"
)

// TODO: implement false positive rate calculation
// TODO: test bloom filter with different sizes
// TODO: implement multiple hash functions
// TODO: use uint8 instead of bool - takes less memory but more cpu time, verify this

var hasher []hash.Hash32

func init() {
	hasher = []hash.Hash32{
		murmur3.New32WithSeed(uint32(10)),
		murmur3.New32WithSeed(uint32(20000)),
	}
}

func murmurhash(hasherIdx int, key string, size uint32) int {
	defer hasher[hasherIdx].Reset()
	hasher[hasherIdx].Write([]byte(key))
	result := hasher[hasherIdx].Sum32() % size
	return int(result)
}

type BloomFilter struct {
	filter []uint8
	size   uint32
}

func NewBloomFilter(size uint32) *BloomFilter {
	return &BloomFilter{
		filter: make([]uint8, size),
		size:   size,
	}
}

func (b *BloomFilter) Add(key string) {
	for i := range hasher {
		idx := murmurhash(i, key, b.size)
		aIdx := idx / 8
		bIdx := idx % 8
		b.filter[aIdx] = b.filter[aIdx] | (1 << bIdx)
	}
}

func (b *BloomFilter) Exists(key string) (int, bool) {
	for i := range hasher {
		idx := murmurhash(i, key, b.size)
		aIdx := idx / 8
		bIdx := idx % 8
		if b.filter[aIdx]&(1<<bIdx) <= 0 {
			return idx, false
		}
	}
	return 0, true
}

func main() {

	dataset := make([]string, 0)
	existsMap := make(map[string]bool)

	for range 500 {
		unique := uuid.New().String()
		existsMap[unique] = true
		dataset = append(dataset, unique)
	}

	for range 500 {
		unique := uuid.New().String()
		existsMap[unique] = false
		dataset = append(dataset, unique)
	}
	for bloomSize := 1000; bloomSize < 10000; bloomSize += 100 {

		bloom := NewBloomFilter(uint32(bloomSize))
		for _, key := range dataset {
			if existsMap[key] {
				bloom.Add(key)
			}
		}
		falsePositives := 0
		for _, key := range dataset {
			if !existsMap[key] {
				_, exists := bloom.Exists(key)
				if exists {
					falsePositives++
				}
			}
		}
		fmt.Println(bloomSize, " : ", 100*(float64(falsePositives)/float64(len(dataset))))
	}
}

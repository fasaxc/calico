package interncache

import (
	"hash/maphash"
	"runtime"
	"sync"
	"weak"
)

const (
	defaultNumBuckets = 1 << 10
	growPercent       = 70
	shrinkPercent     = 30
)

type Cache[T any] struct {
	seed   maphash.Seed
	hashFn func(maphash.Seed, *T) uint64
	equals func(*T, *T) bool

	lock sync.Mutex
	m    [][]weak.Pointer[T]
	len  int
}

func New[T any](
	hashFn func(maphash.Seed, *T) uint64,
	equalsFn func(*T, *T) bool,
) *Cache[T] {
	return &Cache[T]{
		seed:   maphash.MakeSeed(),
		hashFn: hashFn,
		equals: equalsFn,
		m:      make([][]weak.Pointer[T], defaultNumBuckets),
	}
}

func (c *Cache[T]) Intern(v *T) *T {
	h := c.hash(v)
	bucketIdx := h % uint64(len(c.m))

	c.lock.Lock()
	defer c.lock.Unlock()
	bucket := c.m[bucketIdx]
	updatedBucket := bucket[:0]
	var foundValue *T
	for _, p := range bucket {
		internedValue := p.Value()
		if internedValue == nil {
			c.len--
			continue
		}
		updatedBucket = append(updatedBucket, p)
		if foundValue != nil {
			continue
		}
		if c.equals(internedValue, v) {
			foundValue = internedValue
		}
	}
	if foundValue == nil {
		updatedBucket = append(updatedBucket, weak.Make(v))
		c.len++
		foundValue = v
		runtime.AddCleanup(v, c.gcNilsInBucket, h)
	}
	c.m[bucketIdx] = updatedBucket
	c.maybeResize()
	return foundValue
}

func (c *Cache[T]) Len() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.len
}

func (c *Cache[T]) maybeResize() {
	if c.len > len(c.m)*growPercent/100 {
		c.resize(len(c.m) * 2)
	} else if c.len > defaultNumBuckets && c.len < len(c.m)*shrinkPercent/100 {
		c.resize(len(c.m) * 2)
	}
}

func (c *Cache[T]) resize(newSize int) {
	oldM := c.m
	c.m = make([][]weak.Pointer[T], newSize)
	c.len = 0
	for _, bucket := range oldM {
		for _, p := range bucket {
			value := p.Value()
			if value == nil {
				continue
			}

			h := c.hash(value)
			bucketIdx := h % uint64(len(c.m))
			c.m[bucketIdx] = append(c.m[bucketIdx], p)
			c.len++
		}
	}
}

func (c *Cache[T]) gcNilsInBucket(h uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	bucketIdx := h % uint64(len(c.m))
	bucket := c.m[bucketIdx]
	updatedBucket := bucket[:0]
	for _, p := range bucket {
		if p.Value() == nil {
			c.len--
			continue
		}
		updatedBucket = append(updatedBucket, p)
	}
	c.m[bucketIdx] = updatedBucket
	c.maybeResize()
}

func (c *Cache[T]) hash(v *T) uint64 {
	return c.hashFn(c.seed, v)
}

package lsmtree

type BloomFilter struct {
	bitSet        []bool
	hashFuncitons []func(string) uint
}

func NewBloomFilter(size int, hashFuncs ...func(string) uint) *BloomFilter {
	return &BloomFilter{
		bitSet:        make([]bool, size),
		hashFuncitons: hashFuncs,
	}
}

func (bf *BloomFilter) Add(key string) {
	for _, hashFunc := range bf.hashFuncitons {
		index := hashFunc(key) % uint(len(key))
		bf.bitSet[index] = true
	}
}

func (bf *BloomFilter) MightContain(key string) bool {
	for _, hashFunc := range bf.hashFuncitons {
		index := hashFunc(key) % uint(len(key))
		if !bf.bitSet[index] {
			return false
		}
	}
	return true
}

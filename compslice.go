package compexperiments

import (
	"unsafe"
)

//go:generate go run gen/gendeltapack.go gen/genhelper.go
//go:generate gofmt -w deltapack_gen.go

const (
	groupSize = 64
)

type CompressedSlice[T PackType] struct {
	// compressed buffer
	buf []uint64
	// uncompressed tail
	tail []T
	// block offsets
	blockOffsets []int64
	// optional min and max values
	minMax []minMax[T]
}

func (cs *CompressedSlice[T]) Import(buf []uint64, tail []T, blockOffsets []int64, minMax []minMax[T]) {
	cs.buf = buf
	cs.tail = tail
	cs.blockOffsets = blockOffsets
}

func (cs *CompressedSlice[T]) Export() ([]uint64, []T, []int64, []minMax[T]) {
	return cs.buf, cs.tail, cs.blockOffsets, cs.minMax
}

func (cs *CompressedSlice[T]) WithMinMax(v bool) {
	if v {
		cs.minMax = make([]minMax[T], 0)
	} else {
		cs.minMax = nil
	}
}

func (cs *CompressedSlice[T]) Len() int {
	if cs.BlockCount() == 0 {
		return 0
	}
	if len(cs.tail) > 0 {
		return len(cs.tail) + cs.blockOffset(cs.BlockCount()-1)
	}
	return cs.blockOffset(cs.BlockCount())
}

func (cs *CompressedSlice[T]) CompressedSize() int {
	return len(cs.buf)*8 + len(cs.blockOffsets)*8 + len(cs.tail)*int(unsafe.Sizeof(T(0)))
}

func (cs *CompressedSlice[T]) MemSize() int {
	return int(unsafe.Sizeof(cs)) + cap(cs.buf)*8 + cap(cs.tail)*int(unsafe.Sizeof(T(0))) + cap(cs.blockOffsets)*8 + cap(cs.minMax)*int(unsafe.Sizeof(minMax[T]{}))
}

func (cs *CompressedSlice[T]) FirstValue() T {
	if len(cs.blockOffsets) > 0 {
		return *(*T)(unsafe.Pointer(&cs.buf[0]))
	}
	if len(cs.tail) > 0 {
		return cs.tail[0]
	}
	panic("empty slice")
}

func (cs *CompressedSlice[T]) LastValue() T {
	if len(cs.tail) > 0 {
		return cs.tail[len(cs.tail)-1]
	}
	if len(cs.blockOffsets) > 0 {
		// need to uncompressed last block
		lastBlock, _ := cs.GetBlock(nil, len(cs.blockOffsets)-1)
		return lastBlock[len(lastBlock)-1]
	}
	panic("empty slice")
}

func (cs *CompressedSlice[T]) IsBlockCompressed(i int) bool {
	return i < len(cs.blockOffsets)
}

func (cs *CompressedSlice[T]) BlockCount() int {
	if len(cs.tail) > 0 {
		return len(cs.blockOffsets) + 1
	}
	return len(cs.blockOffsets)
}

func (cs *CompressedSlice[T]) BlockLen(i int) int {
	if i < len(cs.blockOffsets) {
		if i < MaxGroups {
			return (i + 1) * groupSize
		}
		return MaxGroups * groupSize
	}
	if i == len(cs.blockOffsets) && len(cs.tail) > 0 {
		return len(cs.tail)
	}
	panic("invalid block index")
}

func (cs *CompressedSlice[T]) BlockFirstValue(i int) T {
	if i < len(cs.blockOffsets) {
		return *(*T)(unsafe.Pointer(&cs.buf[cs.blockOffsets[i]]))
	}
	if i == len(cs.blockOffsets) && len(cs.tail) > 0 {
		return cs.tail[0]
	}
	panic("invalid block index")
}

func (cs *CompressedSlice[T]) BlockMinMax(i int) (T, T) {
	if cs.minMax == nil {
		panic("minmax not enabled")
	}
	if i < len(cs.minMax) {
		return cs.minMax[i].min, cs.minMax[i].max
	}
	if i == len(cs.minMax) && len(cs.tail) > 0 {
		// find min and max of tail
		min, max := cs.tail[0], cs.tail[0]
		for _, v := range cs.tail {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		return min, max
	}
	panic("invalid block index")
}

func (cs CompressedSlice[T]) Append(src []T) CompressedSlice[T] {
	cs.add(src, 0)
	return cs
}

func (cs CompressedSlice[T]) AppendLossy(src []T, maxBits int) CompressedSlice[T] {
	minNtz := cs.fractionSize() - maxBits
	if minNtz < 1 {
		panic("maxBits too large")
	}
	cs.add(src, minNtz)
	return cs
}

func (cs CompressedSlice[T]) AppendOne(src T) CompressedSlice[T] {
	cs.addOne(src, 0)
	return cs
}

func (cs CompressedSlice[T]) AppendOneLossy(src T, maxBits int) CompressedSlice[T] {
	minNtz := cs.fractionSize() - maxBits
	if minNtz < 1 {
		panic("maxBits too large")
	}
	cs.addOne(src, minNtz)
	return cs
}

func (cs *CompressedSlice[T]) Add(src []T) {
	cs.add(src, 0)
}

func (cs *CompressedSlice[T]) AddLossy(src []T, maxBits int) {
	minNtz := cs.fractionSize() - maxBits
	if minNtz < 1 {
		panic("maxBits too large")
	}
	cs.add(src, minNtz)
}

func (cs *CompressedSlice[T]) AddOne(src T) {
	cs.addOne(src, 0)
}

func (cs *CompressedSlice[T]) AddOneLossy(src T, maxBits int) {
	minNtz := cs.fractionSize() - maxBits
	if minNtz < 1 {
		panic("maxBits too large")
	}
	cs.addOne(src, minNtz)
}

func (cs *CompressedSlice[T]) add(src []T, minNtz int) {
	// Compute nbGroups:
	// first block should have 1 group
	// second block should have 2 groups
	// third block should have 3 groups
	// later blocks should have 4 groups
	nbGroups := len(cs.blockOffsets)
	for len(src) > 0 {
		nbGroups++
		if nbGroups > MaxGroups {
			nbGroups = MaxGroups
		}
		if len(cs.tail) > 0 {
			appendTailCount := groupSize*nbGroups - len(cs.tail)
			if appendTailCount > len(src) {
				appendTailCount = len(src)
			}
			cs.tail = append(cs.tail, src[:appendTailCount]...)
			src = src[appendTailCount:]
			if len(cs.tail) == groupSize*nbGroups {
				// tail is full, compress it
				cs.addBlock(cs.tail, minNtz)
				cs.tail = nil
			}
		} else if len(src) >= groupSize*nbGroups {
			// compress a full block
			cs.addBlock(src[:groupSize*nbGroups], minNtz)
			src = src[groupSize*nbGroups:]
		} else {
			// append to tail
			cs.tail = append(cs.tail, src...)
			src = nil
		}
	}
}

func (cs *CompressedSlice[T]) addOne(src T, minNtz int) {
	nbGroups := len(cs.blockOffsets) + 1
	if nbGroups > MaxGroups {
		nbGroups = MaxGroups
	}
	cs.tail = append(cs.tail, src)
	if len(cs.tail) == groupSize*nbGroups {
		// tail is full, compress it
		cs.addBlock(cs.tail, minNtz)
		// reset tail
		// the client is using addOne to add one value at a time
		// so it's better to alloc preemtively a new tail to avoid
		// reallocations
		nbGroups++
		if nbGroups > MaxGroups {
			nbGroups = MaxGroups
		}
		cs.tail = make([]T, 0, groupSize*nbGroups)
	}
}

func (cs *CompressedSlice[T]) addBlock(block []T, minNtz int) {
	blockOffsetvalue := block[0]
	var bh BlockHeader
	BlockHeaderPos := int64(len(cs.buf))
	// append block header (blockOffsetValue + block header)
	cs.buf = append(cs.buf, *(*uint64)(unsafe.Pointer(&blockOffsetvalue)))
	cs.buf = append(cs.buf, bh[:]...)
	if cs.minMax != nil {
		min, max := block[0], block[0]
		for _, v := range block {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		cs.minMax = append(cs.minMax, minMax[T]{min, max})
	}
	for len(block) > 0 {
		var bitlen, ntz int
		group := (*[groupSize]T)(block)
		switch any(T(0)).(type) {
		case float32, float64:
			cs.buf, bitlen, ntz, _ = compressGroupXorAppend(cs.buf, group, blockOffsetvalue, minNtz)
		default:
			cs.buf, bitlen, ntz, _ = compressGroupDeltaAppend(cs.buf, group, blockOffsetvalue)
		}
		blockOffsetvalue = group[groupSize-1]
		bh = bh.AddGroup(bitlen, ntz)
		block = block[groupSize:]
	}
	*(*BlockHeader)(cs.buf[BlockHeaderPos+1:]) = bh
	cs.blockOffsets = append(cs.blockOffsets, BlockHeaderPos)

}

func (cs *CompressedSlice[T]) Get(dst []T) []T {
	if cap(dst) == 0 {
		dst = make([]T, 0, cs.Len())
	}
	blockCount := cs.BlockCount()
	for i := 0; i < blockCount; i++ {
		dst, _ = cs.GetBlock(dst, i)
	}
	return dst
}

func (cs *CompressedSlice[T]) GetBlock(dst []T, i int) ([]T, int) {
	if cap(dst) == 0 {
		sz := cs.Len()
		if sz > MaxGroups*groupSize {
			sz = MaxGroups * groupSize
		}
		dst = make([]T, 0, sz)
	}
	if i < len(cs.blockOffsets) {
		blockOffset := cs.blockOffsets[i]
		blockOffsetvalue := *(*T)(unsafe.Pointer(&cs.buf[blockOffset]))
		bh := *(*BlockHeader)(cs.buf[blockOffset+1:])
		in := cs.buf[cs.blockOffsets[i]+1+int64(len(bh)):]
		nbGroups := bh.GroupCount()
		for i := 0; i < nbGroups; i++ {
			bitlen, ntz := bh.GetGroup(i)
			switch any(T(0)).(type) {
			case float32, float64:
				dst = decompressGroupXorAppend(dst, in, blockOffsetvalue, bitlen, ntz)
			default:
				dst = decompressGroupDeltaAppend(dst, in, blockOffsetvalue, bitlen, ntz)
			}
			blockOffsetvalue = dst[len(dst)-1]
			in = in[bitlen-ntz:]
		}
		return dst, cs.blockOffset(i)
	}
	if i == len(cs.blockOffsets) && len(cs.tail) > 0 {
		return append(dst, cs.tail...), cs.blockOffset(i)
	}
	panic("invalid block index")
}

func (cs *CompressedSlice[T]) blockOffset(i int) int {
	var res int
	for g := 1; g < MaxGroups; g++ {
		if g > i {
			return res
		}
		res += g * groupSize
	}
	return res + (i-MaxGroups+1)*MaxGroups*groupSize
}

func (cs CompressedSlice[T]) fractionSize() int {
	switch any(T(0)).(type) {
	case float32:
		// size of float32 fraction is 23 bits
		// https://en.wikip
		return 23
	case float64:
		// size of float64 fraction is 52 bits
		// https://en.wikipedia.org/wiki/Double-precision_floating-point_format
		return 52
	default:
		panic("fractionSize applies only to float types")
	}
}

type minMax[T PackType] struct {
	min T
	max T
}

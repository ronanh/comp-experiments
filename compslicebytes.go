package compexperiments

import (
	"errors"
	"io"
	"unsafe"
)

type BytesSlice struct {
	buf     []byte
	offsets []int64
}

func (bs *BytesSlice) Len() int {
	return len(bs.offsets)
}

func (bs *BytesSlice) Value(i int) []byte {
	if i+1 < len(bs.offsets) {
		return bs.buf[bs.offsets[i]:bs.offsets[i+1]]
	}
	return bs.buf[bs.offsets[i]:]
}

func (bs *BytesSlice) Values(dst [][]byte) [][]byte {
	if len(bs.offsets) == 0 {
		return nil
	}
	iFirst := len(dst)
	if cap(dst)-len(dst) < len(bs.offsets) {
		dstCopy := dst
		dst = make([][]byte, len(bs.offsets)+len(dst))
		copy(dst, dstCopy)
	} else {
		dst = dst[:len(bs.offsets)+len(dst)]
	}
	for i := range bs.offsets {
		dst[i+iFirst] = bs.blockBuf(i)
	}
	return dst
}

func (bs *BytesSlice) blockBuf(i int) []byte {
	if i+1 < len(bs.offsets) {
		return bs.buf[bs.offsets[i]:bs.offsets[i+1]]
	}
	return bs.buf[bs.offsets[i]:]
}

func (bs *BytesSlice) ValuesBytes() ([]byte, []int64) {
	return bs.buf, bs.offsets
}

func (bs *BytesSlice) Reset() {
	bs.buf = bs.buf[:0]
	bs.offsets = bs.offsets[:0]
}

type CompressedBytesSlice struct {
	// compressed buffer of concatenated bytes
	buf []byte
	// uncompressed tail of concatenated bytes
	tail []byte
	// compressed block offsets
	bufBlockOffsets []int64
	// offsets within concatenated bytes
	offsets CompressedSlice[int64]
	// last offset for concatenated bytes
	lastOffset int64
}

func (cs *CompressedBytesSlice) Import(buf []byte, tail []byte, bufBlockOffsets []int64, offsets CompressedSlice[int64]) {
	cs.buf = buf
	cs.tail = tail
	cs.bufBlockOffsets = bufBlockOffsets
	cs.offsets = offsets
	// compute last offset
	if offsets.Len() > 0 {
		block, _ := offsets.GetBlock(nil, offsets.BlockCount()-1)
		cs.lastOffset = block[len(block)-1]
	}
}

func (cs *CompressedBytesSlice) Export() ([]byte, []byte, []int64, CompressedSlice[int64]) {
	return cs.buf, cs.tail, cs.bufBlockOffsets, cs.offsets
}

func (cs *CompressedBytesSlice) Len() int {
	return cs.offsets.Len()
}

func (cs *CompressedBytesSlice) DataLen() int {
	return int(cs.lastOffset)
}

func (cs *CompressedBytesSlice) CompressedSize() int {
	return len(cs.buf) + len(cs.tail) + len(cs.bufBlockOffsets)*8 + cs.offsets.CompressedSize()
}

func (cs *CompressedBytesSlice) MemSize() int {
	return int(unsafe.Sizeof(cs)) + cap(cs.buf) + cap(cs.tail) + cap(cs.bufBlockOffsets)*8 + cs.offsets.MemSize()
}

func (cs *CompressedBytesSlice) IsBlockCompressed(iBlock int) bool {
	return cs.offsets.IsBlockCompressed(iBlock)
}

func (cs *CompressedBytesSlice) BlockCount() int {
	return cs.offsets.BlockCount()
}

// BlockNum returns the block (iBlock) given the index in the uncompressed slice
func (cs *CompressedBytesSlice) BlockNum(i int) int {
	return cs.offsets.BlockNum(i)
}

func (cs *CompressedBytesSlice) BlockLen(iBlock int) int {
	return cs.offsets.BlockLen(iBlock)
}

func (cs *CompressedBytesSlice) BlockDataLen(iBlock int) int {
	endOffset := cs.lastOffset
	if iBlock+1 < cs.BlockCount() {
		endOffset = cs.offsets.BlockFirstValue(iBlock + 1)
	}
	return int(endOffset - cs.offsets.BlockFirstValue(iBlock))
}

func (cs CompressedBytesSlice) Append(src [][]byte, encoder any) CompressedBytesSlice {
	cs.Add(src, encoder)
	return cs
}

func (cs *CompressedBytesSlice) Add(src [][]byte, encoder any) {
	newOffsets := make([]int64, len(src))
	curOffset := cs.lastOffset
	for i, v := range src {
		newOffsets[i] = curOffset
		curOffset += int64(len(v))
	}
	cs.lastOffset = curOffset
	firstCompressedBlock := cs.offsets.BlockCount() - 1
	if firstCompressedBlock >= 0 && !cs.offsets.IsBlockCompressed(firstCompressedBlock) {
		firstCompressedBlock--
	}
	firstCompressedBlock++

	cs.offsets.Add(newOffsets)
	originalTail := cs.tail
	// Use sliceOffsets new compressed blocks to add the corresponding data blocks
	blockCount := cs.offsets.BlockCount()
	for i := firstCompressedBlock; i < blockCount; i++ {
		if !cs.offsets.IsBlockCompressed(i) {
			break
		}
		start := cs.offsets.BlockFirstValue(i)
		end := curOffset
		if i+1 < blockCount {
			end = cs.offsets.BlockFirstValue(i + 1)
		}
		blockLen := end - start
		for int64(len(cs.tail)) < blockLen {
			cs.tail = append(cs.tail, src[0]...)
			src = src[1:]
		}
		if int64(len(cs.tail)) != blockLen {
			panic("invalid tail length")
		}
		cs.addBlock(cs.tail, encoder)
		if len(originalTail) > 0 && sameSlice(cs.tail, originalTail) {
			// should not modify the original tail data
			cs.tail = nil
		} else {
			cs.tail = cs.tail[:0]
		}
	}
	// Add the remaining input to the tail
	for _, v := range src {
		cs.tail = append(cs.tail, v...)
	}

	if len(cs.bufBlockOffsets) != len(cs.offsets.blockOffsets) {
		panic("invalid block offsets")
	}
}

func (cs CompressedBytesSlice) AppendOne(src []byte, encoder any) CompressedBytesSlice {
	cs.AddOne(src, encoder)
	return cs
}

func (cs *CompressedBytesSlice) AddOne(src []byte, encoder any) {
	lastBlock := cs.offsets.BlockCount() - 1
	wasCompressed := cs.offsets.IsBlockCompressed(lastBlock)

	// add offset
	cs.offsets.AddOne(cs.lastOffset)
	cs.lastOffset += int64(len(src))

	// append to src to tail
	cs.tail = append(cs.tail, src...)

	if cs.offsets.IsBlockCompressed(lastBlock) && !wasCompressed {
		// one more compressed block
		// -> compress tail
		cs.addBlock(cs.tail, encoder)
		// reset tail
		// the client is using addOne to add one value at a time
		// so it's better to alloc preemtively a new tail to avoid
		// reallocations
		cs.tail = make([]byte, 0, len(cs.tail))
	}
}

func (cs CompressedBytesSlice) AppendBytes(src []byte, offsets []int64, encoder any) CompressedBytesSlice {
	cs.AddBytes(src, offsets, encoder)
	return cs
}

func (cs *CompressedBytesSlice) AddBytes(src []byte, offsets []int64, encoder any) {
	if len(cs.bufBlockOffsets) != len(cs.offsets.blockOffsets) {
		panic("invalid block offsets")
	}
	prevBlockCount := cs.offsets.BlockCount()
	withTail := len(cs.offsets.tail) > 0

	if cs.lastOffset != 0 {
		unmodifiedOffsets := offsets
		offsets = make([]int64, len(offsets))
		for i, v := range unmodifiedOffsets {
			offsets[i] = cs.lastOffset + v
		}
	}
	cs.offsets.Add(offsets)
	cs.lastOffset += int64(len(src))

	newBlockCount := cs.offsets.BlockCount()

	// if tail is not empty, try to fill it first
	// to make a complete block
	if withTail {
		firstBlock := prevBlockCount - 1
		// blockStart, blockEnd := cs.blockOffsetRange(firstBlock)
		appendSize := cs.BlockDataLen(firstBlock) - len(cs.tail)
		cs.tail = append(cs.tail, src[:appendSize]...)
		src = src[appendSize:]
		if cs.offsets.IsBlockCompressed(firstBlock) {
			cs.addBlock(cs.tail, encoder)
			cs.tail = nil
		}
	}
	for i := prevBlockCount; i < newBlockCount; i++ {
		if !cs.offsets.IsBlockCompressed(i) {
			// add the remaining input to the tail
			cs.tail = append(cs.tail, src...)
			src = nil
			if i != newBlockCount-1 {
				panic("invalid block count")
			}
			break
		}
		blockSize := cs.BlockDataLen(i)

		cs.addBlock(src[:blockSize], encoder)
		src = src[blockSize:]
	}
	if len(src) > 0 {
		panic("src should have been consumed")
	}
	if len(cs.bufBlockOffsets) != len(cs.offsets.blockOffsets) {
		panic("invalid block offsets")
	}
}

func (cp *CompressedBytesSlice) addBlock(block []byte, encoder any) {
	// Compress offset block
	cp.bufBlockOffsets = append(cp.bufBlockOffsets, int64(len(cp.buf)))
	// compress data block
	var err error
	cp.buf, err = encode(cp.buf, block, encoder)
	if err != nil {
		panic(err)
	}
}

func (cs *CompressedBytesSlice) Get(dst BytesSlice, decoder any) BytesSlice {
	if len(cs.bufBlockOffsets) != len(cs.offsets.blockOffsets) {
		panic("invalid block offsets")
	}
	dst.buf, dst.offsets = cs.GetBytes(dst.buf, dst.offsets, decoder)
	return dst
}

func (cs *CompressedBytesSlice) GetBytes(dst []byte, dstOffsets []int64, decoder any) ([]byte, []int64) {
	if len(cs.bufBlockOffsets) != len(cs.offsets.blockOffsets) {
		panic("invalid block offsets")
	}
	if cap(dst) == 0 {
		dst = make([]byte, 0, cs.DataLen())
	}
	if cap(dstOffsets) == 0 {
		dstOffsets = make([]int64, 0, cs.offsets.Len())
	}
	blockCount := cs.BlockCount()
	for i := 0; i < blockCount; i++ {
		dst, dstOffsets, _ = cs.GetBlockBytes(dst, dstOffsets, i, decoder)
	}
	return dst, dstOffsets
}

func (cs *CompressedBytesSlice) GetBlock(dst BytesSlice, iBlock int, decoder any) (BytesSlice, int) {
	if len(cs.bufBlockOffsets) != len(cs.offsets.blockOffsets) {
		panic("invalid block offsets")
	}
	var blockOffset int
	dst.buf, dst.offsets, blockOffset = cs.GetBlockBytes(dst.buf, dst.offsets, iBlock, decoder)
	return dst, blockOffset
}

func (cs *CompressedBytesSlice) GetBlockBytes(dst []byte, dstOffsets []int64, iBlock int, decoder any) ([]byte, []int64, int) {
	if len(cs.bufBlockOffsets) != len(cs.offsets.blockOffsets) {
		panic("invalid block offsets")
	}
	if cap(dst) == 0 {
		dst = make([]byte, 0, cs.BlockDataLen(iBlock))
	}
	if cap(dstOffsets) == 0 {
		dstOffsets = make([]int64, 0, cs.BlockLen(iBlock))
	}

	blockOffsetPos := len(dstOffsets)
	firstBlockOffset := int64(len(dst))
	dstOffsets, _ = cs.offsets.GetBlock(dstOffsets, iBlock)
	// Fix offsets
	if delta := dstOffsets[blockOffsetPos] - firstBlockOffset; delta != 0 {
		for j := blockOffsetPos; j < len(dstOffsets); j++ {
			dstOffsets[j] -= delta
		}
	}

	// Decompress data
	if cs.IsBlockCompressed(iBlock) {
		var err error
		dst, err = decode(dst, cs.blockBuf(iBlock), decoder)
		if err != nil {
			panic(err)
		}
	} else {
		// last block is uncompressed
		dst = append(dst, cs.tail...)
	}
	return dst, dstOffsets, cs.offsets.blockOffset(iBlock)
}

func (cs *CompressedBytesSlice) blockBuf(iBlock int) []byte {
	if iBlock+1 < len(cs.bufBlockOffsets) {
		return cs.buf[cs.bufBlockOffsets[iBlock]:cs.bufBlockOffsets[iBlock+1]]
	}
	return cs.buf[cs.bufBlockOffsets[iBlock]:]
}

func sameSlice(x, y []byte) bool {
	return len(x) == len(y) && &x[0] == &y[0]
}

func encode(dst, src []byte, encoder any) ([]byte, error) {
	if enc, ok := encoder.(zstdEncoder); ok {
		return enc.EncodeAll(src, dst), nil
	} else if enc, ok := encoder.(iguanaEncoder); ok {
		return enc.Compress(src, dst, 1.0)
	}
	return nil, errors.New("unknown encoder")
}

func decode(dst, src []byte, decoder any) ([]byte, error) {
	if dec, ok := decoder.(zstdDecoder); ok {
		return dec.DecodeAll(src, dst)
	} else if dec, ok := decoder.(iguanaDecoder); ok {
		return dec.DecompressTo(src, dst)
	}
	return nil, errors.New("unknown decoder")
}

type zstdEncoder interface {
	EncodeAll(src []byte, dst []byte) []byte
	Flush() error
	Reset(w io.Writer)
}

type zstdDecoder interface {
	DecodeAll(input []byte, dst []byte) ([]byte, error)
}

type iguanaEncoder interface {
	Compress(src []byte, dst []byte, ansRejectionThreshold float32) ([]byte, error)
}

type iguanaDecoder interface {
	DecompressTo(src []byte, dst []byte) ([]byte, error)
}

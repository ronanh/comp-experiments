package compexperiments

import (
	"io"
	"unsafe"
)

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

type BytesSlice struct {
	buf     []byte
	offsets []int
	values  [][]byte
}

func (bs *BytesSlice) Len() int {
	return len(bs.offsets)
}

func (cs *CompressedBytesSlice) DataLen() int {
	return cs.lastOffset
}

func (bs *BytesSlice) Value(i int) []byte {
	if i == len(bs.offsets)-1 {
		return bs.buf[bs.offsets[i]:]
	}
	return bs.buf[bs.offsets[i]:bs.offsets[i+1]]
}

func (bs *BytesSlice) Values() [][]byte {
	if len(bs.offsets) == 0 {
		return nil
	}
	if cap(bs.values) < len(bs.offsets) {
		bs.values = make([][]byte, len(bs.offsets))
	} else {
		bs.values = bs.values[:len(bs.offsets)]
	}
	for i := range bs.values {
		if i == len(bs.values)-1 {
			bs.values[i] = bs.buf[bs.offsets[i]:]
		} else {
			bs.values[i] = bs.buf[bs.offsets[i]:bs.offsets[i+1]]
		}
	}
	return bs.values
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
	compressedBlockOffsets []int
	// offsets within concatenated bytes
	offsets CompressedSlice[int]
	// last offset for concatenated bytes
	lastOffset int
}

func (cs *CompressedBytesSlice) Len() int {
	return cs.offsets.Len()
}

func (cs *CompressedBytesSlice) CompressedSize() int {
	return len(cs.buf) + len(cs.tail) + len(cs.compressedBlockOffsets)*8 + cs.offsets.CompressedSize()
}

func (cs *CompressedBytesSlice) MemSize() int {
	return int(unsafe.Sizeof(cs)) + cap(cs.buf) + cap(cs.tail) + cap(cs.compressedBlockOffsets)*8 + cs.offsets.MemSize()
}

func (cp *CompressedBytesSlice) IsBlockCompressed(i int) bool {
	return cp.offsets.IsBlockCompressed(i)
}

func (cs *CompressedBytesSlice) BlockCount() int {
	return cs.offsets.BlockCount()
}

func (cp *CompressedBytesSlice) BlockLen(i int) int {
	return cp.offsets.BlockLen(i)
}

func (cs CompressedBytesSlice) Compress(src [][]byte, encoder any) CompressedBytesSlice {
	newOffsets := make([]int, len(src))
	curOffset := cs.lastOffset
	for i, v := range src {
		newOffsets[i] = curOffset
		curOffset += len(v)
	}
	cs.lastOffset = curOffset
	firstCompressedBlock := cs.offsets.BlockCount() - 1
	if firstCompressedBlock >= 0 && !cs.offsets.IsBlockCompressed(firstCompressedBlock) {
		firstCompressedBlock--
	}
	firstCompressedBlock++

	cs.offsets = cs.offsets.Compress(newOffsets)
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
		for len(cs.tail) < blockLen {
			cs.tail = append(cs.tail, src[0]...)
			src = src[1:]
		}
		if len(cs.tail) != blockLen {
			panic("invalid tail length")
		}
		cs.compressBlock(cs.tail, encoder)
		if len(originalTail) > 0 && sameSlice(cs.tail, originalTail) {
			// we should not modify the original tail data
			cs.tail = nil
		} else {
			cs.tail = cs.tail[:0]
		}
	}
	// Add the remaining input to the tail
	for _, v := range src {
		cs.tail = append(cs.tail, v...)
	}

	return cs
}

func (cp *CompressedBytesSlice) compressBlock(block []byte, encoder any) {
	// Compress offset block
	cp.compressedBlockOffsets = append(cp.compressedBlockOffsets, len(cp.buf))
	// compress data block
	var err error
	if enc, ok := encoder.(zstdEncoder); ok {
		cp.buf = enc.EncodeAll(block, cp.buf)
	} else if enc, ok := encoder.(iguanaEncoder); ok {
		cp.buf, err = enc.Compress(block, cp.buf, 1.0)
	} else {
		panic("unknown encoder")
	}
	if err != nil {
		panic(err)
	}
}

func (cs *CompressedBytesSlice) Decompress(dst BytesSlice, decoder any) BytesSlice {
	blockCount := cs.BlockCount()
	for i := 0; i < blockCount; i++ {
		dst, _ = cs.DecompressBlock(dst, i, decoder)
	}
	return dst
}

func (cs *CompressedBytesSlice) DecompressBlock(dst BytesSlice, i int, decoder any) (BytesSlice, int) { // Decompress offsets
	blockOffsetPos := len(dst.offsets)
	firstBlockOffset := len(dst.buf)
	dst.offsets, _ = cs.offsets.DecompressBlock(dst.offsets, i)
	// Fix offsets
	if delta := dst.offsets[blockOffsetPos] - firstBlockOffset; delta != 0 {
		for j := blockOffsetPos; j < len(dst.offsets); j++ {
			dst.offsets[j] -= delta
		}
	}

	// Decompress data
	if cs.IsBlockCompressed(i) {
		var blockBuf []byte
		if i+1 < len(cs.compressedBlockOffsets) {
			blockBuf = cs.buf[cs.compressedBlockOffsets[i]:cs.compressedBlockOffsets[i+1]]
		} else {
			blockBuf = cs.buf[cs.compressedBlockOffsets[i]:]
		}
		var err error
		if dec, ok := decoder.(zstdDecoder); ok {
			dst.buf, err = dec.DecodeAll(blockBuf, dst.buf)
		} else if dec, ok := decoder.(iguanaDecoder); ok {
			dst.buf, err = dec.DecompressTo(dst.buf, blockBuf)
		} else {
			panic("unknown decoder")
		}
		if err != nil {
			panic(err)
		}
	} else {
		// last block is uncompressed
		dst.buf = append(dst.buf, cs.tail...)
	}

	return dst, cs.blockOffset(i)
}

func (cs *CompressedBytesSlice) blockOffset(i int) int {
	return cs.offsets.blockOffset(i)
}

func sameSlice(x, y []byte) bool {
	return len(x) == len(y) && &x[0] == &y[0]
}

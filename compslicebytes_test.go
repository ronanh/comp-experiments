package compexperiments_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/klauspost/compress/zstd"
	compexperiments "github.com/ronanh/compexperiments"
)

func TestCompressBytes(t *testing.T) {
	testInput1, testInput2 := genTestInputs(1000), genTestInputs(1000)

	var cs compexperiments.CompressedBytesSlice

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		t.Fatalf("expected no error")
	}
	defer enc.Close()
	cs = cs.Compress(testInput1, enc)
	if cs.Len() != len(testInput1) {
		t.Fatalf("expected same len")
	}

	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatalf("expected no error")
	}
	defer dec.Close()
	var dst compexperiments.BytesSlice
	dst = cs.Decompress(dst, dec)
	if dst.Len() != len(testInput1) {
		t.Fatalf("expected same len")
	}
	cs = cs.Compress(testInput2, enc)
	if cs.Len() != len(testInput1)+len(testInput2) {
		t.Fatalf("expected same len")
	}
	dst.Reset()
	dst = cs.Decompress(dst, dec)
	if dst.Len() != len(testInput1)+len(testInput2) {
		t.Fatalf("expected same len")
	}
	testInput := append(testInput1, testInput2...)
	var values [][]byte
	for i := 0; i < cs.BlockCount(); i++ {
		dst.Reset()
		var off int
		dst, off = cs.DecompressBlock(dst, i, dec)
		for j := 0; j < dst.Len(); j++ {
			if !bytes.Equal(dst.Value(j), testInput[off+j]) {
				t.Fatalf("expected same bytes")
			}
		}
		values = dst.Values(values[:0])
		for j, v := range values {
			if !bytes.Equal(v, testInput[off+j]) {
				t.Fatalf("expected same bytes")
			}
		}
	}
}

// generate random strings (alphanum) of length 0-1000
// converted to []byte
func genTestInputs(n int) [][]byte {
	res := make([][]byte, n)
	for i := range res {
		res[i] = make([]byte, rand.Intn(1000))
		for j := range res[i] {
			// 48-57, 65-90, 97-122
			res[i][j] = byte(rand.Intn(74) + 48)
		}
	}
	return res
}

package decode

import (
	"encoding/hex"
	"errors"
	"math/bits"
	"strings"
)

func Hex(s string, n int) ([]byte, error) {
	if !strings.HasPrefix(s, "0x") {
		return nil, errors.New("missing hex prefix")
	}
	b, e := hex.DecodeString(s[2:])
	if e != nil || n >= 0 && len(b) != n {
		return nil, errors.New("invalid hex length/encoding")
	}
	return b, nil
}
func Bitlist(s string) ([]bool, error) {
	b, e := Hex(s, -1)
	if e != nil || len(b) == 0 || b[len(b)-1] == 0 {
		return nil, errors.New("invalid SSZ bitlist terminator")
	}
	n := (len(b)-1)*8 + bits.Len8(b[len(b)-1]) - 1
	out := make([]bool, n)
	for i := range out {
		out[i] = b[i/8]&(1<<uint(i%8)) != 0
	}
	return out, nil
}
func Bitvector(s string, n uint64) ([]uint64, error) {
	if n == 0 || n > 1024 {
		return nil, errors.New("invalid committee preset")
	}
	b, e := Hex(s, int((n+7)/8))
	if e != nil {
		return nil, e
	}
	var out []uint64
	for i := uint64(0); i < uint64(len(b))*8; i++ {
		if b[i/8]&(1<<uint(i%8)) != 0 {
			if i >= n {
				return nil, errors.New("bitvector padding set")
			}
			out = append(out, i)
		}
	}
	return out, nil
}

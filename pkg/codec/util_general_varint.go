package codec

import "encoding/binary"

func putVarint64(bytes []byte, idx int, x int64) int {
	delta := binary.PutVarint(bytes[idx:], x)
	return idx + delta
}

func readVarint64(bytes []byte, idx int) (int64, int) {
	varint, i := binary.Varint(bytes[idx:])
	return varint, idx + i
}

func putUVarint(bytes []byte, idx int, x uint32) int {
	i := binary.PutUvarint(bytes[idx:], uint64(x))
	return idx + i
}

func readUVarint(bytes []byte, idx int) (uint32, int) {
	varint, i := binary.Uvarint(bytes[idx:])
	return uint32(varint), idx + i
}

func putVarint(bytes []byte, idx int, x int) int {
	delta := binary.PutVarint(bytes[idx:], int64(x))
	return idx + delta
}

func readVarint(bytes []byte, idx int) (int, int) {
	varint, i := binary.Varint(bytes[idx:])
	return int(varint), idx + i
}

//nolint
func varint64Size(x int64) int {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	i := 0
	for x >= 0x80 {
		x >>= 7
		i++
	}
	return i + 1
}

func varintSize(x int) int {
	return varint64Size(int64(x))
}

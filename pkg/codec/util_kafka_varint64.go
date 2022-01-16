package codec

// This file is for kafka code varint64 type. Format method as alpha order.

func putRelativeTimestamp(bytes []byte, idx int, x int64) int {
	return putVarint64(bytes, idx, x)
}

func readRelativeTimestamp(bytes []byte, idx int) (int64, int) {
	return readVarint64(bytes, idx)
}

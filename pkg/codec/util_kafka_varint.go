package codec

// This file is for kafka code varint type. Format method as alpha order.

func putRelativeOffset(bytes []byte, idx int, x int) int {
	return putVarint(bytes, idx, x)
}

func readRelativeOffset(bytes []byte, idx int) (int, int) {
	return readVarint(bytes, idx)
}

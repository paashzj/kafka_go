package codec

// This file is for kafka code int64 type. Format method as alpha order.

func putLogStartOffset(bytes []byte, idx int, x int64) int {
	return putInt64(bytes, idx, x)
}

func readLogStartOffset(bytes []byte, idx int) (int64, int) {
	return readInt64(bytes, idx)
}

func putOffset(bytes []byte, idx int, x int64) int {
	return putInt64(bytes, idx, x)
}

func readOffset(bytes []byte, idx int) (int64, int) {
	return readInt64(bytes, idx)
}

func putProducerId(bytes []byte, idx int, x int64) int {
	return putInt64(bytes, idx, x)
}

func readProducerId(bytes []byte, idx int) (int64, int) {
	return readInt64(bytes, idx)
}

func putRetentionTime(bytes []byte, idx int, x int64) int {
	return putInt64(bytes, idx, x)
}

func readRetentionTime(bytes []byte, idx int) (int64, int) {
	return readInt64(bytes, idx)
}

func putSessionLifeTimeout(bytes []byte, idx int, ms int64) int {
	return putInt64(bytes, idx, ms)
}

func putTime(bytes []byte, idx int, x int64) int {
	return putInt64(bytes, idx, x)
}

func readTime(bytes []byte, idx int) (int64, int) {
	return readInt64(bytes, idx)
}

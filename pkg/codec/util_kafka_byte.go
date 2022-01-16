package codec

func putCoordinatorType(bytes []byte, idx int, x byte) int {
	return putByte(bytes, idx, x)
}

func readCoordinatorType(bytes []byte, idx int) (byte, int) {
	return bytes[idx], idx + 1
}

func putIsolationLevel(bytes []byte, idx int, x byte) int {
	return putByte(bytes, idx, x)
}

func readIsolationLevel(bytes []byte, idx int) (byte, int) {
	return bytes[idx], idx + 1
}

func putMagicByte(bytes []byte, idx int, x byte) int {
	return putByte(bytes, idx, x)
}

func readMagicByte(bytes []byte, idx int) (byte, int) {
	return bytes[idx], idx + 1
}

func putRecordAttributes(bytes []byte, idx int, x byte) int {
	return putByte(bytes, idx, x)
}

func readRecordAttributes(bytes []byte, idx int) (byte, int) {
	return bytes[idx], idx + 1
}

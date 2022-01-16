package codec

// This file is for kafka code int type. Format method as alpha order.

func putBrokerPort(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readBrokerPort(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putClusterAuthorizedOperation(bytes []byte, idx int, corrId int) int {
	return putInt(bytes, idx, corrId)
}

func putCorrId(bytes []byte, idx int, corrId int) int {
	return putInt(bytes, idx, corrId)
}

func readCorrId(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putGenerationId(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readGenerationId(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putLastOffsetDelta(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readLastOffsetDelta(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putMessageSize(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readMessageSize(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putPartitionId(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readPartitionId(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putThrottleTime(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func putTopicAuthorizedOperation(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

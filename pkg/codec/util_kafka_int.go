package codec

// This file is for kafka code int type. Format method as alpha order.

func putBaseSequence(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readBaseSequence(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putBrokerNodeId(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readBrokerNodeId(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putBrokerPort(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readBrokerPort(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putTopicAuthorizedOperation(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
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

func putLeaderId(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readLeaderId(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putLastOffsetDelta(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func putLeaderEpoch(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readLeaderEpoch(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putMessageSize(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func putPartitionId(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readPartitionId(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putReplicaId(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

func readReplicaId(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putThrottleTime(bytes []byte, idx int, x int) int {
	return putInt(bytes, idx, x)
}

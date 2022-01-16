package codec

// This file is for kafka code int32 type. Format method as alpha order.

func putBaseSequence(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readBaseSequence(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putBrokerNodeId(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readBrokerNodeId(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putControllerId(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readControllerId(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putFetchSessionEpoch(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readFetchSessionEpoch(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putLeaderEpoch(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readLeaderEpoch(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putLeaderId(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readLeaderId(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putNodeId(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readNodeId(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

func putReplicaId(bytes []byte, idx int, x int32) int {
	return putInt32(bytes, idx, x)
}

func readReplicaId(bytes []byte, idx int) (int32, int) {
	return readInt32(bytes, idx)
}

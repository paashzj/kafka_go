package codec

// This file is for kafka code string type. Format method as alpha order.

func putClientId(bytes []byte, idx int, str string) int {
	return putString(bytes, idx, str)
}

func readClientId(bytes []byte, idx int) (string, int) {
	return readString(bytes, idx)
}

func readClientSoftwareName(bytes []byte, idx int) (string, int) {
	return readCompactString(bytes, idx)
}

func readClientSoftwareVersion(bytes []byte, idx int) (string, int) {
	return readCompactString(bytes, idx)
}

func putClusterId(bytes []byte, idx int, str string) int {
	return putCompactString(bytes, idx, str)
}

func readClusterId(bytes []byte, idx int) (string, int) {
	return readCompactString(bytes, idx)
}

func readCoordinatorKey(bytes []byte, idx int) (string, int) {
	return readCompactString(bytes, idx)
}

func putErrorMessageString(bytes []byte, idx int, str string) int {
	return putString(bytes, idx, str)
}

func readErrorMessageString(bytes []byte, idx int) (string, int) {
	return readString(bytes, idx)
}

func putErrorMessage(bytes []byte, idx int, str string) int {
	return putCompactString(bytes, idx, str)
}

func readErrorMessage(bytes []byte, idx int) (string, int) {
	return readCompactString(bytes, idx)
}

func readFindCoordinatorErrorMessage(bytes []byte, idx int) (*string, int) {
	return readCompactStringNullable(bytes, idx)
}

func putFindCoordinatorErrorMessage(bytes []byte, idx int, str *string) int {
	return putCompactStringNullable(bytes, idx, str)
}

func putHostString(bytes []byte, idx int, str string) int {
	return putString(bytes, idx, str)
}

func putHost(bytes []byte, idx int, str string) int {
	return putCompactString(bytes, idx, str)
}

func putGroupId(bytes []byte, idx int, str string) int {
	return putCompactString(bytes, idx, str)
}

func readGroupId(bytes []byte, idx int) (string, int) {
	return readCompactString(bytes, idx)
}

func putGroupIdString(bytes []byte, idx int, str string) int {
	return putString(bytes, idx, str)
}

func readGroupIdString(bytes []byte, idx int) (string, int) {
	return readString(bytes, idx)
}

func putGroupInstanceId(bytes []byte, idx int, str *string) int {
	return putCompactStringNullable(bytes, idx, str)
}

func readGroupInstanceId(bytes []byte, idx int) (*string, int) {
	return readCompactStringNullable(bytes, idx)
}

func putGroupLeaderId(bytes []byte, idx int, str string) int {
	return putCompactString(bytes, idx, str)
}

func readGroupLeaderId(bytes []byte, idx int, version int) (string, int) {
	return readCompactString(bytes, idx)
}

func putGroupLeaderIdString(bytes []byte, idx int, str string) int {
	return putString(bytes, idx, str)
}

func readGroupLeaderIdString(bytes []byte, idx int, version int) (string, int) {
	return readString(bytes, idx)
}

func putMemberId(bytes []byte, idx int, str string) int {
	return putCompactString(bytes, idx, str)
}

func readMemberId(bytes []byte, idx int) (string, int) {
	return readCompactString(bytes, idx)
}

func putMemberIdString(bytes []byte, idx int, str string) int {
	return putString(bytes, idx, str)
}

func readMemberIdString(bytes []byte, idx int) (string, int) {
	return readString(bytes, idx)
}

func putMetadata(bytes []byte, idx int, str *string) int {
	return putCompactStringNullable(bytes, idx, str)
}

func readMetadata(bytes []byte, idx int) (*string, int) {
	return readCompactStringNullable(bytes, idx)
}

func putProtocolNameNullable(bytes []byte, idx int, str *string) int {
	return putCompactStringNullable(bytes, idx, str)
}

func readProtocolNameNullable(bytes []byte, idx int) (*string, int) {
	return readCompactStringNullable(bytes, idx)
}

func putProtocolName(bytes []byte, idx int, str string) int {
	return putCompactString(bytes, idx, str)
}

func readProtocolName(bytes []byte, idx int) (string, int) {
	return readCompactString(bytes, idx)
}

func putProtocolNameString(bytes []byte, idx int, str string) int {
	return putString(bytes, idx, str)
}

func readProtocolNameString(bytes []byte, idx int) (string, int) {
	return readString(bytes, idx)
}

func putProtocolType(bytes []byte, idx int, str string) int {
	return putCompactString(bytes, idx, str)
}

func readProtocolType(bytes []byte, idx int) (string, int) {
	return readCompactString(bytes, idx)
}

func putProtocolTypeString(bytes []byte, idx int, str string) int {
	return putString(bytes, idx, str)
}

func readProtocolTypeString(bytes []byte, idx int) (string, int) {
	return readString(bytes, idx)
}

func readProtocolTypeNullable(bytes []byte, idx int) (*string, int) {
	return readCompactStringNullable(bytes, idx)
}

func putProtocolTypeNullable(bytes []byte, idx int, str *string) int {
	return putCompactStringNullable(bytes, idx, str)
}

func putRack(bytes []byte, idx int, str *string) int {
	return putCompactStringNullable(bytes, idx, str)
}

func readRack(bytes []byte, idx int) (*string, int) {
	return readCompactStringNullable(bytes, idx)
}

func putSaslMechanism(bytes []byte, idx int, str string) int {
	return putString(bytes, idx, str)
}

func readSaslMechanism(bytes []byte, idx int) (string, int) {
	return readString(bytes, idx)
}

func putTopicString(bytes []byte, idx int, str string) int {
	return putString(bytes, idx, str)
}

func readTopicString(bytes []byte, idx int) (string, int) {
	return readString(bytes, idx)
}

func putTopic(bytes []byte, idx int, str string) int {
	return putCompactString(bytes, idx, str)
}

func readTopic(bytes []byte, idx int) (string, int) {
	return readCompactString(bytes, idx)
}

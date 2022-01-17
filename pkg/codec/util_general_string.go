package codec

func readCompactStringNullable(bytes []byte, idx int) (*string, int) {
	strLen, idx := readUVarint(bytes, idx)
	if strLen == 0 {
		return nil, idx
	}
	intLen := int(strLen)
	s := string(bytes[idx : idx+intLen-1])
	return &s, idx + intLen
}

func putCompactStringNullable(bytes []byte, idx int, str *string) int {
	if str == nil {
		return putUVarint(bytes, idx, 0)
	}
	idx = putUVarint(bytes, idx, uint32(CompactStrLen(*str)))
	copy(bytes[idx:], *str)
	return idx + len(*str)
}

func readCompactString(bytes []byte, idx int) (string, int) {
	strLen, _ := readUVarint(bytes, idx)
	intLen := int(strLen)
	return string(bytes[idx+1 : idx+intLen]), idx + intLen
}

func putCompactString(bytes []byte, idx int, str string) int {
	idx = putUVarint(bytes, idx, uint32(CompactStrLen(str)))
	copy(bytes[idx:], str)
	return idx + len(str)
}

func readString(bytes []byte, idx int) (string, int) {
	length, idx := readInt16(bytes, idx)
	intLen := int(length)
	return string(bytes[idx : idx+intLen]), idx + intLen
}

func putString(bytes []byte, idx int, str string) int {
	strBytes := []byte(str)
	idx = putInt16(bytes, idx, int16(len(strBytes)))
	copy(bytes[idx:idx+len(strBytes)], strBytes)
	return idx + len(strBytes)
}

func readNullableString(bytes []byte, idx int) (*string, int) {
	length, idx := readInt16(bytes, idx)
	if length == -1 {
		return nil, idx + 2
	}
	intLen := int(length)
	aux := string(bytes[idx : idx+intLen])
	return &aux, idx + intLen
}

func putNullableString(bytes []byte, idx int, str *string) int {
	if str == nil {
		return putInt16(bytes, idx, -1)
	}
	strBytes := []byte(*str)
	idx = putInt16(bytes, idx, int16(len(strBytes)))
	copy(bytes[idx:idx+len(strBytes)], strBytes)
	return idx + len(strBytes)
}

func StrLen(str string) int {
	return 2 + len([]byte(str))
}

func NullableStrLen(str *string) int {
	if str == nil {
		return 2
	}
	return 2 + len([]byte(*str))
}

func CompactStrLen(str string) int {
	return 1 + len([]byte(str))
}

func CompactNullableStrLen(str *string) int {
	if str == nil {
		return 1
	}
	return 1 + len([]byte(*str))
}

package codec

type RecordBatch struct {
	Offset      int64
	MessageSize int
	LeaderEpoch int32
	MagicByte   byte
	// 8位flag字节
	Flags uint16

	LastOffsetDelta int
	FirstTimestamp  int64
	LastTimestamp   int64
	ProducerId      int64
	ProducerEpoch   int16
	BaseSequence    int32
	Records         []*Record
}

func DecodeRecordBatch(bytes []byte, version int16) *RecordBatch {
	recordBatch := &RecordBatch{}
	idx := 0
	recordBatch.Offset, idx = readOffset(bytes, idx)
	recordBatch.MessageSize, idx = readMessageSize(bytes, idx)
	recordBatch.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
	recordBatch.MagicByte, idx = readMagicByte(bytes, idx)
	// todo now we skip the crc32
	idx += 4
	recordBatch.Flags, idx = readUInt16(bytes, idx)
	recordBatch.LastOffsetDelta, idx = readLastOffsetDelta(bytes, idx)
	recordBatch.FirstTimestamp, idx = readTime(bytes, idx)
	recordBatch.LastTimestamp, idx = readTime(bytes, idx)
	recordBatch.ProducerId, idx = readProducerId(bytes, idx)
	recordBatch.ProducerEpoch, idx = readProducerEpoch(bytes, idx)
	recordBatch.BaseSequence, idx = readBaseSequence(bytes, idx)
	var length int
	length, idx = readInt(bytes, idx)
	recordBatch.Records = make([]*Record, length)
	for i := 0; i < length; i++ {
		recordLength, idx := readVarint(bytes, idx)
		record := DecodeRecord(bytes[idx:idx+recordLength-1], version)
		idx += recordLength
		recordBatch.Records[i] = record
	}
	return recordBatch
}

func (r *RecordBatch) BytesLength() int {
	result := LenOffset + LenMessageSize + LenLeaderEpoch + LenMagicByte + LenCrc32
	// record batch flags
	result += 2
	result += LenOffsetDelta
	result += LenTime
	result += LenTime
	result += LenProducerId
	result += LenProducerEpoch
	result += LenBaseSequence
	result += LenArray
	for _, record := range r.Records {
		// record size
		bytesLength := record.BytesLength()
		result += varintSize(bytesLength)
		result += bytesLength
	}
	return result
}

func (r *RecordBatch) Bytes() []byte {
	bytes := make([]byte, r.BytesLength())
	idx := 0
	idx = putOffset(bytes, idx, r.Offset)
	idx = putMessageSize(bytes, idx, r.MessageSize)
	idx = putLeaderEpoch(bytes, idx, r.LeaderEpoch)
	bytes[idx] = r.MagicByte
	idx++
	crc32Start := idx
	// crc32后面计算
	idx += 4
	idx = putUInt16(bytes, idx, r.Flags)
	idx = putLastOffsetDelta(bytes, idx, r.LastOffsetDelta)
	idx = putTime(bytes, idx, r.FirstTimestamp)
	idx = putTime(bytes, idx, r.LastTimestamp)
	idx = putProducerId(bytes, idx, r.ProducerId)
	idx = putProducerEpoch(bytes, idx, r.ProducerEpoch)
	idx = putBaseSequence(bytes, idx, r.BaseSequence)
	idx = putArrayLen(bytes, idx, len(r.Records))
	for _, record := range r.Records {
		idx = putRecord(bytes, idx, record.Bytes())
	}
	putCrc32(bytes[crc32Start:], bytes[crc32Start+4:])
	return bytes
}

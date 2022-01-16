package service

type RecordBatch struct {
	Offset          int64
	MessageSize     int
	LastOffsetDelta int
	FirstTimestamp  int64
	LastTimestamp   int64
	BaseSequence    int32
	Records         []*Record
}

type Record struct {
	RelativeTimestamp int64
	RelativeOffset    int
	Key               []byte
	Value             string
	Headers           []byte
}

package codec

const (
	LenCorrId               = 4
	LenErrorCode            = 2
	LenArray                = 4
	LenApiV0                = 6
	LenApiV3                = 7
	LenThrottleTime         = 4
	LenFetchSessionId       = 4
	LenPartitionId          = 4
	LenOffset               = 8
	LenLastStableOffset     = 8
	LenStartOffset          = 8
	LenAbortTransactions    = 4
	LenReplicaId            = 4
	LenMessageSize          = 4
	LenNodeId               = 4
	LenPort                 = 4
	LenGenerationId         = 4
	LenTime                 = 8
	LenLeaderId             = 4
	LenLeaderEpoch          = 4
	LenControllerId         = 4
	LenIsInternalV9         = 4
	LenIsInternalV1         = 1
	LenTopicAuthOperation   = 4
	LenClusterAuthOperation = 4
	LenRecordAttributes     = 1
	LenMagicByte            = 1
	LenCrc32                = 4
	LenOffsetDelta          = 4
	LenProducerId           = 8
	LenProducerEpoch        = 2
	LenBaseSequence         = 4
	LenSessionTimeout       = 8
	LenBatchIndex           = 4
)

const LenTaggedField = 1

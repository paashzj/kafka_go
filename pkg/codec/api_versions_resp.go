package codec

import (
	"github.com/paashzj/kafka_go/pkg/codec/api"
)

type ApiResponse struct {
	BaseResp
	ErrorCode       int16
	ApiRespVersions []*ApiRespVersion
	ThrottleTime    int
}

type ApiRespVersion struct {
	ApiKey     api.Code
	MinVersion int16
	MaxVersion int16
}

func NewApiVersionResp(version int16, corrId int) *ApiResponse {
	resp := ApiResponse{}
	resp.CorrelationId = corrId
	resp.ErrorCode = 0
	resp.ApiRespVersions = buildApiRespVersions()
	resp.ThrottleTime = 0
	return &resp
}

func (a *ApiResponse) BytesLength(version int16) int {
	length := LenCorrId + LenErrorCode + LenArray
	if version == 0 {
		length += LenApiV0 * len(a.ApiRespVersions)
	}
	if version == 3 {
		length += LenApiV3*len(a.ApiRespVersions) + LenThrottleTime + 1
	}
	return length
}

func (a *ApiResponse) Bytes(version int16) []byte {
	bytes := make([]byte, a.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, a.CorrelationId)
	idx = putErrorCode(bytes, idx, a.ErrorCode)
	if version == 0 {
		idx = putArrayLen(bytes, idx, len(a.ApiRespVersions))
	}
	if version == 3 {
		idx = putCompactArrayLen(bytes, idx, len(a.ApiRespVersions))
	}
	for i := 0; i < len(a.ApiRespVersions); i++ {
		apiRespVersion := a.ApiRespVersions[i]
		idx = putApiKey(bytes, idx, apiRespVersion.ApiKey)
		idx = putApiMinVersion(bytes, idx, apiRespVersion.MinVersion)
		idx = putApiMaxVersion(bytes, idx, apiRespVersion.MaxVersion)
		if version == 3 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 3 {
		idx = putThrottleTime(bytes, idx, a.ThrottleTime)
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}

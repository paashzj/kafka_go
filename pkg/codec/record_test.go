// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecodeRecordCase2(t *testing.T) {
	bytes := testHex2Bytes(t, "000000011053686f6f74487a6a00")
	record := DecodeRecord(bytes, 1)
	var expectedAttr byte = 0
	assert.Equal(t, expectedAttr, record.RecordAttributes)
	var expectedTs int64 = 0
	assert.Equal(t, expectedTs, record.RelativeTimestamp)
	assert.Equal(t, 0, record.RelativeOffset)
	assert.Nil(t, record.Key)
	assert.Equal(t, []byte("ShootHzj"), record.Value)
	assert.Nil(t, record.Headers)
}

func TestCodeRecord(t *testing.T) {
	record := &Record{}
	record.RecordAttributes = 0
	record.RelativeTimestamp = 0
	record.RelativeOffset = 11111
	record.Key = nil
	record.Value = []byte("SSXVNJHPDQDXVCRASTVYBCWVMGNYKRXVZXKGXTSPSJDGYLUEGQFLAQLOCFLJBEPOWFNSOMYARHAOPUFOJHHDXEHXJBHWGSMZJGNLONJVXZXZOZITKXJBOZWDJMCBOSYQQKCPRRDCZWMRLFXBLGQPRPGRNTAQOOSVXPKJPJLAVSQCCRXFRROLLHWHOHFGCFWPNDLMWCSSHWXQQYKALAAWCMXYLMZALGDESKKTEESEMPRHROVKUMPSXHELIDQEOOHOIHEGJOAZBVPUMCHSHGXZYXXQRUICRIJGQEBBWAXABQRIRUGZJUUVFYQOVCDEDXYFPRLGSGZXSNIAVODTJKSQWHNWVPSAMZKOUDTWHIORJSCZIQYPCZMBYWKDIKOKYNGWPXZWMKRDCMBXKFUILWDHBFXRFAOPRUGDFLPDLHXXCXCUPLWGDPPHEMJGMTVMFQQFVCUPOFYWLDUEBICKPZKHKVMCJVWVKTXBKAPWAPENUEZNWNWDCACDRLPIPHJQQKMOFDQSPKKNURFBORJLBPCBIWTSJNPRBNITTKJYWAHWGKZYNUSFISPIYPIOGAUPZDXHCFVGXGIVVCPFHIXAACZXZLFDMOOSSNTKUPJQEIRRQAMUCTBLBSVPDDYOIHAOODZNJTVHDCIEGTAVMYZOCIVSKUNSMXEKBEWNZPRPWPUJABJXNQBOXSHOEGMJSNBUTGTIFVEQPSYBDXEXORPQDDODZGBELOISTRWXMEYWVVHGMJKWLJCCHPKAFRASZEYQZCVLFSLOWTLBMPPWPPFPQSAZPTULSTCDMODYKZGSRFQTRFTGCNMNXQQIYVUQZHVNIPHZWVBSGOBYIFDNNXUTBBQUYNXOZCSICGRTZSSRHROJRGBHMHEQJRDLOQBEPTOBMYLMIGPPDPOLTEUVDGATCGYPQOGOYYESKEGBLOCBIYSLQEYGCCIPBXPNSPKDYTBEWDHBHWVDPLOVHJPNYGJUHKKHDASNFGZDAIWWQEPPBRJKDGOSAFAPRLWFFXRVMZQTKRYF")
	record.Headers = nil
	record.Bytes()
	assert.Equal(t, 1033, record.BytesLength())
}

func TestCodeRecordCase2(t *testing.T) {
	record := &Record{}
	record.RecordAttributes = 0
	record.RelativeTimestamp = 0
	record.RelativeOffset = 0
	record.Key = nil
	record.Value = []byte("ShootHzj")
	record.Headers = nil
	bytes := record.Bytes()
	expectBytes := testHex2Bytes(t, "000000011053686f6f74487a6a00")
	assert.Equal(t, expectBytes, bytes)
}

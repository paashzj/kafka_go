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

func TestCrc32Compute(t *testing.T) {
	bytes := testHex2Bytes(t, "000000000000000000090000017f402390a10000017f402390a1ffffffffffffffffffffffffffff0000000a54000000004861646564663461352d333762642d346139332d623861372d6236303765393564373766380054000002004838643534663238662d633231342d346661612d393763632d3530336534373532323235310054000004004866386637363432332d653163342d343634612d396432642d3664313766313962643435310054000006004833333438333766642d316366362d343733302d396530642d3630343462363833363933340054000008004837353434353862332d383530342d343934392d393861342d623263303061323438656563005400000a004864323037326161632d373264362d343038662d623032372d373564363337386261663630005400000c004831353539343137382d316637302d343062612d626338322d313762323437303937343663005400000e004863363935323636352d343065392d346230312d383038382d6663386362393933616464610054000010004838636531366639652d656632312d343534372d383136642d6635353062373338613766300054000012004837366335383139332d663964342d346630612d613833312d63626239313835303439396200")
	aux := make([]byte, 4)
	putCrc32(aux, bytes[4:])
	assert.Equal(t, testHex2Bytes(t, "d96924fb"), aux)
}

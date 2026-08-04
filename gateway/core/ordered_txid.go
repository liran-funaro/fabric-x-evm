/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
)

// OrderedBlockTxIDs extracts the committer TxIDs of every MESSAGE envelope in a
// pre-validation ordered block delivered from the assembler. It does not use
// BlockParser.Parse (which requires TRANSACTIONS_FILTER metadata that
// pre-validation blocks lack); appearance in an ordered block is the gate's
// signal, so validity is irrelevant here. Config/malformed envelopes are
// skipped. The returned TxIDs match those computed by committerTxID from each
// submitted proposal (packager.CreateTx preserves the channel header's TxId).
func OrderedBlockTxIDs(b *common.Block) []string {
	if b == nil || b.Data == nil {
		return nil
	}
	ids := make([]string, 0, len(b.Data.Data))
	for _, raw := range b.Data.Data {
		env, err := protoutil.UnmarshalEnvelope(raw)
		if err != nil {
			continue
		}
		payload, err := protoutil.UnmarshalPayload(env.Payload)
		if err != nil || payload.Header == nil {
			continue
		}
		chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			continue
		}
		if common.HeaderType(chdr.Type) != common.HeaderType_MESSAGE {
			continue
		}
		ids = append(ids, chdr.TxId)
	}
	return ids
}

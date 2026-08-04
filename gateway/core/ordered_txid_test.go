package core

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func envWithTxID(t *testing.T, txID string, typ common.HeaderType) []byte {
	chdr := &common.ChannelHeader{Type: int32(typ), TxId: txID, ChannelId: "ch"}
	payload := &common.Payload{Header: &common.Header{ChannelHeader: protoutil.MarshalOrPanic(chdr)}}
	env := &common.Envelope{Payload: protoutil.MarshalOrPanic(payload)}
	b, err := proto.Marshal(env)
	require.NoError(t, err)
	return b
}

func TestOrderedBlockTxIDs_MessageOnly(t *testing.T) {
	blk := &common.Block{Data: &common.BlockData{Data: [][]byte{
		envWithTxID(t, "tx-1", common.HeaderType_MESSAGE),
		envWithTxID(t, "cfg", common.HeaderType_CONFIG),
		envWithTxID(t, "tx-2", common.HeaderType_MESSAGE),
	}}}
	require.Equal(t, []string{"tx-1", "tx-2"}, OrderedBlockTxIDs(blk))
}

func TestOrderedBlockTxIDs_NilAndMalformedSafe(t *testing.T) {
	require.Nil(t, OrderedBlockTxIDs(nil))
	require.Nil(t, OrderedBlockTxIDs(&common.Block{}))
	blk := &common.Block{Data: &common.BlockData{Data: [][]byte{{0x01, 0x02}}}} // garbage
	require.Empty(t, OrderedBlockTxIDs(blk))
}

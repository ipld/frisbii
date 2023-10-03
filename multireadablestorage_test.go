package frisbii_test

import (
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/frisbii"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	trustlesstestutil "github.com/ipld/go-trustless-utils/testutil"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestMultiReadableStorage(t *testing.T) {
	req := require.New(t)

	blocks := make([]blk, 100)
	for ii := 0; ii < 100; ii++ {
		blocks[ii] = randBlock()
	}
	multistore := frisbii.NewMultiReadableStorage()
	for ii := 0; ii < 5; ii++ {
		bag := make(map[string][]byte)
		for jj := ii * 20; jj < (ii+1)*20; jj++ {
			bag[blocks[jj].cid.KeyString()] = blocks[jj].byts
		}
		if ii == 2 {
			multistore.AddStore(&OnlyStreamingStore{&trustlesstestutil.CorrectedMemStore{ParentStore: &memstore.Store{Bag: bag}}}, []cid.Cid{blocks[ii*20].cid})
		} else {
			multistore.AddStore(&trustlesstestutil.CorrectedMemStore{ParentStore: &memstore.Store{Bag: bag}}, []cid.Cid{blocks[ii*20].cid})
		}
	}
	lsys := cidlink.DefaultLinkSystem()
	lsys.TrustedStorage = true
	lsys.SetReadStorage(multistore)

	for ii := 0; ii < 100; ii++ {
		has, err := multistore.Has(context.Background(), blocks[ii].cid.KeyString())
		req.NoError(err)
		req.True(has)
		byts, err := multistore.Get(context.Background(), blocks[ii].cid.KeyString())
		req.NoError(err)
		req.Equal(blocks[ii].byts, byts)
		byts, err = lsys.LoadRaw(linking.LinkContext{}, cidlink.Link{Cid: blocks[ii].cid})
		req.NoError(err)
		req.Equal(blocks[ii].byts, byts)
	}

	has, err := multistore.Has(context.Background(), randBlock().cid.KeyString())
	req.NoError(err)
	req.False(has)
	_, err = multistore.Get(context.Background(), randBlock().cid.KeyString())
	req.ErrorIs(err, format.ErrNotFound{})
	_, err = lsys.LoadRaw(linking.LinkContext{}, cidlink.Link{Cid: randBlock().cid})
	req.ErrorIs(err, format.ErrNotFound{})
}

type blk struct {
	cid  cid.Cid
	byts []byte
}

func randBlock() blk {
	data := make([]byte, 1024)
	rand.Read(data)
	h, err := mh.Sum(data, mh.SHA2_512, -1)
	if err != nil {
		panic(err)
	}
	return blk{cid.NewCidV1(cid.Raw, h), data}
}

type OnlyStreamingStore struct {
	parent storage.StreamingReadableStorage
}

func (oss *OnlyStreamingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	return oss.parent.GetStream(ctx, key)
}

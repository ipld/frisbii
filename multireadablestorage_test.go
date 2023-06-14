package frisbii_test

import (
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	mh "github.com/multiformats/go-multihash"
	"github.com/rvagg/go-frisbii"
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
			multistore.AddStore(&OnlyStreamingStore{&CorrectedMemStore{Store: &memstore.Store{Bag: bag}}}, []cid.Cid{blocks[ii*20].cid})
		} else {
			multistore.AddStore(&CorrectedMemStore{Store: &memstore.Store{Bag: bag}}, []cid.Cid{blocks[ii*20].cid})
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

var _ storage.StreamingReadableStorage = (*CorrectedMemStore)(nil)
var _ storage.ReadableStorage = (*CorrectedMemStore)(nil)
var _ storage.StreamingReadableStorage = (*OnlyStreamingStore)(nil)

type CorrectedMemStore struct {
	*memstore.Store
}

func (cms *CorrectedMemStore) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := cms.Store.Get(ctx, key)
	if err != nil && err.Error() == "404" {
		err = format.ErrNotFound{}
	}
	return data, err
}

func (cms *CorrectedMemStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := cms.Store.GetStream(ctx, key)
	if err != nil && err.Error() == "404" {
		err = format.ErrNotFound{}
	}
	return rc, err
}

type OnlyStreamingStore struct {
	parent storage.StreamingReadableStorage
}

func (oss *OnlyStreamingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	return oss.parent.GetStream(ctx, key)
}

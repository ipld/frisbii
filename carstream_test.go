package frisbii_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	gstestutil "github.com/ipfs/go-graphsync/testutil"
	"github.com/ipld/go-car/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/rvagg/go-frisbii"
	"github.com/stretchr/testify/require"
)

func TestStreamCar(t *testing.T) {
	ctx := context.Background()
	req := require.New(t)

	store := &CorrectedMemStore{Store: &memstore.Store{
		Bag: make(map[string][]byte),
	}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	tbc := gstestutil.SetupBlockChain(ctx, t, lsys, 1000, 100)
	rootCid := tbc.TipLink.(cidlink.Link).Cid
	allBlocks := tbc.AllBlocks()

	// explore-all, all blocks
	var buf bytes.Buffer
	err := frisbii.StreamCar(ctx, lsys, rootCid, selectorparse.CommonSelector_ExploreAllRecursively, &buf, false)
	req.NoError(err)

	rdr, err := car.NewBlockReader(&buf)
	req.NoError(err)
	req.Equal([]cid.Cid{rootCid}, rdr.Roots)
	var blkCount int
	for {
		blk, err := rdr.Next()
		if err == io.EOF {
			break
		}
		req.NoError(err)
		req.Equal(allBlocks[blkCount], blk)
		blkCount++
	}
	req.Equal(len(allBlocks), blkCount)

	// match-point, just one block
	buf.Reset()
	err = frisbii.StreamCar(ctx, lsys, rootCid, selectorparse.CommonSelector_MatchPoint, &buf, false)
	req.NoError(err)

	rdr, err = car.NewBlockReader(&buf)
	req.NoError(err)
	req.Equal([]cid.Cid{rootCid}, rdr.Roots)
	blkCount = 0
	for {
		blk, err := rdr.Next()
		if err == io.EOF {
			break
		}
		req.NoError(err)
		req.Equal(allBlocks[blkCount], blk)
		blkCount++
	}
	req.Equal(1, blkCount)
}

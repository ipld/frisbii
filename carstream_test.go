package frisbii_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	gstestutil "github.com/ipfs/go-graphsync/testutil"
	unixfs "github.com/ipfs/go-unixfsnode/testutil"
	"github.com/ipld/frisbii"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"
)

func TestStreamCar(t *testing.T) {
	ctx := context.Background()

	chainLsys := makeLsys()
	tbc := gstestutil.SetupBlockChain(ctx, t, chainLsys, 1000, 100)
	allChainBlocks := tbc.AllBlocks()

	fileLsys := makeLsys()
	fileEnt := unixfs.GenerateFile(t, &fileLsys, rand.Reader, 1<<20)

	dirLsys := makeLsys()
	var dirEnt unixfs.DirEntry
	for {
		dirEnt = GenerateNoDupes(func() unixfs.DirEntry { return unixfs.GenerateDirectory(t, &dirLsys, rand.Reader, 4<<20, false) })
		if len(dirEnt.Children) > 2 { // we want at least 3 children to test the path subset selector
			break
		}
	}

	shardedDirLsys := makeLsys()
	var shardedDirEnt unixfs.DirEntry
	for {
		shardedDirEnt = GenerateNoDupes(func() unixfs.DirEntry { return unixfs.GenerateDirectory(t, &shardedDirLsys, rand.Reader, 4<<20, true) })
		if len(dirEnt.Children) > 2 { // we want at least 3 children to test the path subset selector
			break
		}
	}

	testCases := []struct {
		name           string
		selector       datamodel.Node
		root           cid.Cid
		lsys           linking.LinkSystem
		expectedBytes  int64
		expectedBlocks int64
		validate       func(t *testing.T, r io.Reader)
	}{
		{
			name:           "chain: all blocks",
			selector:       selectorparse.CommonSelector_ExploreAllRecursively,
			root:           tbc.TipLink.(cidlink.Link).Cid,
			lsys:           chainLsys,
			expectedBytes:  sizeOf(allChainBlocks),
			expectedBlocks: 100,
			validate: func(t *testing.T, r io.Reader) {
				root, blks := carToBlocks(t, r)
				require.Equal(t, tbc.TipLink.(cidlink.Link).Cid, root)
				require.Equal(t, allChainBlocks, blks)
			},
		},
		{
			name:           "chain: just root",
			selector:       selectorparse.CommonSelector_MatchPoint,
			root:           tbc.TipLink.(cidlink.Link).Cid,
			lsys:           chainLsys,
			expectedBytes:  sizeOf(allChainBlocks[:1]),
			expectedBlocks: 1,
			validate: func(t *testing.T, r io.Reader) {
				root, blks := carToBlocks(t, r)
				require.Equal(t, tbc.TipLink.(cidlink.Link).Cid, root)
				require.Equal(t, []blocks.Block{allChainBlocks[0]}, blks)
			},
		},
		{
			name:           "unixfs file",
			selector:       selectorparse.CommonSelector_ExploreAllRecursively,
			root:           fileEnt.Root,
			lsys:           fileLsys,
			expectedBytes:  sizeOfDirEnt(fileEnt, fileLsys),
			expectedBlocks: int64(len(fileEnt.SelfCids)),
			validate: func(t *testing.T, r io.Reader) {
				root, blks := carToBlocks(t, r)
				require.Equal(t, fileEnt.Root, root)
				require.ElementsMatch(t, fileEnt.SelfCids, blkCids(blks))
			},
		},
		{
			name:           "unixfs directory",
			selector:       selectorparse.CommonSelector_ExploreAllRecursively,
			root:           dirEnt.Root,
			lsys:           dirLsys,
			expectedBytes:  sizeOfDirEnt(dirEnt, dirLsys),
			expectedBlocks: blocksInDirEnt(dirEnt),
			validate: func(t *testing.T, r io.Reader) {
				root, blks := carToBlocks(t, r)
				require.Equal(t, dirEnt.Root, root)
				require.ElementsMatch(t, entCids(dirEnt), blkCids(blks))
			},
		},
		{
			name:           "unixfs sharded directory",
			selector:       selectorparse.CommonSelector_ExploreAllRecursively,
			root:           shardedDirEnt.Root,
			lsys:           shardedDirLsys,
			expectedBytes:  sizeOfDirEnt(shardedDirEnt, shardedDirLsys),
			expectedBlocks: blocksInDirEnt(shardedDirEnt),
			validate: func(t *testing.T, r io.Reader) {
				root, blks := carToBlocks(t, r)
				require.Equal(t, shardedDirEnt.Root, root)
				require.ElementsMatch(t, entCids(shardedDirEnt), blkCids(blks))
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)
			var buf bytes.Buffer
			byts, blks, err := frisbii.StreamCar(ctx, tc.lsys, tc.root, tc.selector, &buf, false)
			req.NoError(err)
			req.Equal(tc.expectedBytes, byts)
			req.Equal(tc.expectedBlocks, blks)
			tc.validate(t, &buf)
		})
	}
}

func carToBlocks(t *testing.T, r io.Reader) (cid.Cid, []blocks.Block) {
	rdr, err := car.NewBlockReader(r)
	require.NoError(t, err)
	require.Len(t, rdr.Roots, 1)
	blks := make([]blocks.Block, 0)
	for {
		blk, err := rdr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		blks = append(blks, blk)
	}
	return rdr.Roots[0], blks
}

func blkCids(blks []blocks.Block) []cid.Cid {
	cids := make([]cid.Cid, len(blks))
	for i, blk := range blks {
		cids[i] = blk.Cid()
	}
	return cids
}

func entCids(ent unixfs.DirEntry) []cid.Cid {
	cids := make([]cid.Cid, 0)
	var _entCids func(ent unixfs.DirEntry)
	_entCids = func(ent unixfs.DirEntry) {
		cids = append(cids, ent.SelfCids...)
		for _, c := range ent.Children {
			_entCids(c)
		}
	}
	_entCids(ent)
	return cids
}

func makeLsys() linking.LinkSystem {
	store := &CorrectedMemStore{Store: &memstore.Store{
		Bag: make(map[string][]byte),
	}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	return lsys
}

// TODO: this should probably be an option in unixfsnode/testutil, for
// generators to strictly not return a DAG with duplicates

func GenerateNoDupes(gen func() unixfs.DirEntry) unixfs.DirEntry {
	var check func(unixfs.DirEntry) bool
	var seen map[cid.Cid]struct{}
	check = func(e unixfs.DirEntry) bool {
		for _, c := range e.SelfCids {
			if _, ok := seen[c]; ok {
				return false
			}
			seen[c] = struct{}{}
		}
		for _, c := range e.Children {
			if !check(c) {
				return false
			}
		}
		return true
	}
	for {
		seen = make(map[cid.Cid]struct{})
		gend := gen()
		if check(gend) {
			return gend
		}
	}
}

func sizeOf(blks []blocks.Block) int64 {
	var size int64
	for _, blk := range blks {
		size += int64(len(blk.RawData()))
	}
	return size
}
func sizeOfDirEnt(dirEnt unixfs.DirEntry, ls linking.LinkSystem) int64 {
	var size int64
	for _, c := range dirEnt.SelfCids {
		blk, err := ls.LoadRaw(linking.LinkContext{}, cidlink.Link{Cid: c})
		if err != nil {
			panic(err)
		}
		size += int64(len(blk))
	}
	for _, c := range dirEnt.Children {
		size += sizeOfDirEnt(c, ls)
	}
	return size
}

func blocksInDirEnt(dirEnt unixfs.DirEntry) int64 {
	size := int64(len(dirEnt.SelfCids))
	for _, c := range dirEnt.Children {
		size += blocksInDirEnt(c)
	}
	return size
}

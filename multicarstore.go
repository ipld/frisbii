package frisbii

import (
	"context"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	provider "github.com/ipni/index-provider"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

type MultiCarStore struct {
	trusted bool
	stores  []carstorage.ReadableCar
	roots   []cid.Cid
	lk      sync.RWMutex
}

func NewMultiCarStore(trusted bool) *MultiCarStore {
	return &MultiCarStore{
		trusted: trusted,
		stores:  make([]carstorage.ReadableCar, 0),
		roots:   make([]cid.Cid, 0),
	}
}

func (m *MultiCarStore) AddStore(store carstorage.ReadableCar) {
	m.lk.Lock()
	defer m.lk.Unlock()
	m.stores = append(m.stores, store)
	m.roots = append(m.roots, store.Roots()...)
}

func (m *MultiCarStore) RootsLister() provider.MultihashLister {
	return func(ctx context.Context, id peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		m.lk.RLock()
		defer m.lk.RUnlock()
		mh := make([]multihash.Multihash, 0, len(m.roots))
		for _, r := range m.roots {
			mh = append(mh, r.Hash())
		}
		return provider.SliceMultihashIterator(mh), nil
	}
}

func (m *MultiCarStore) LinkSystem() linking.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.TrustedStorage = m.trusted
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)

	lsys.StorageReadOpener = func(lctx linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
		m.lk.RLock()
		defer m.lk.RUnlock()
		for _, store := range m.stores {
			rdr, err := store.GetStream(lctx.Ctx, lnk.Binary())
			if err != nil {
				if nf, ok := err.(interface{ NotFound() bool }); ok && nf.NotFound() {
					continue
				}
				return nil, err
			}
			return rdr, nil
		}
		return nil, format.ErrNotFound{Cid: lnk.(cidlink.Link).Cid}
	}

	return lsys
}

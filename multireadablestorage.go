package frisbii

import (
	"context"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime/storage"
	provider "github.com/ipni/index-provider"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

var _ storage.StreamingReadableStorage = (*MultiReadableStorage)(nil)
var _ storage.ReadableStorage = (*MultiReadableStorage)(nil)

// MultiReadableStorage manages a list of storage.StreamingReadableStorage
// stores, providing a unified LinkSystem interface to them.
type MultiReadableStorage struct {
	stores []storage.StreamingReadableStorage
	roots  []cid.Cid
	lk     sync.RWMutex
}

func NewMultiReadableStorage() *MultiReadableStorage {
	return &MultiReadableStorage{
		stores: make([]storage.StreamingReadableStorage, 0),
		roots:  make([]cid.Cid, 0),
	}
}

func (m *MultiReadableStorage) AddStore(store storage.StreamingReadableStorage, roots []cid.Cid) {
	m.lk.Lock()
	defer m.lk.Unlock()
	m.stores = append(m.stores, store)
	m.roots = append(m.roots, roots...)
}

func (m *MultiReadableStorage) RootsLister() provider.MultihashLister {
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

func (m *MultiReadableStorage) Has(ctx context.Context, key string) (bool, error) {
	m.lk.RLock()
	defer m.lk.RUnlock()
	for _, store := range m.stores {
		if hasStore, ok := store.(storage.Storage); ok {
			has, err := hasStore.Has(ctx, key)
			if err != nil {
				return false, err
			}
			if has {
				return true, nil
			}
		} else {
			rdr, err := store.GetStream(ctx, key)
			if err != nil {
				if nf, ok := err.(interface{ NotFound() bool }); ok && nf.NotFound() {
					continue
				}
				return false, err
			}
			rdr.Close()
			return true, nil
		}
	}
	return false, nil
}

func (m *MultiReadableStorage) Get(ctx context.Context, key string) ([]byte, error) {
	if rdr, err := m.GetStream(ctx, key); err != nil {
		return nil, err
	} else {
		return io.ReadAll(rdr)
	}
}

// TODO: store affinity via context? Once we find a store that has the
// block, we should prefer it for the rest of the request.
// TODO: check roots list for the block being requested, if it's in the
// list, we know which store to go to first.

func (m *MultiReadableStorage) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	m.lk.RLock()
	defer m.lk.RUnlock()
	for _, store := range m.stores {
		rdr, err := store.GetStream(ctx, key)
		if err != nil {
			if nf, ok := err.(interface{ NotFound() bool }); ok && nf.NotFound() {
				continue
			}
			return nil, err
		}
		return rdr, nil
	}
	cid, _ := cid.Cast([]byte(key))
	return nil, format.ErrNotFound{Cid: cid}
}

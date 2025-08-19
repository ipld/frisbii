package frisbii

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	// codecs we care about

	_ "github.com/ipld/go-ipld-prime/codec/cbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/json"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
	"github.com/ipld/go-trustless-utils/traversal"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-car/v2/storage/deferred"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	trustlessutils "github.com/ipld/go-trustless-utils"
)

var (
	// ProbeCID is the special identity CID used for probing the gateway
	// bafkqaaa is an identity CID with empty content
	ProbeCID = cid.MustParse("bafkqaaa")

	// probeCarBytes is the pre-generated CAR response for the probe CID.
	// Since this is always the same, we generate it once and reuse it.
	probeCarBytes []byte
	probeCarOnce  sync.Once
)

// getProbeCarBytes generates or returns the cached probe CAR response
func getProbeCarBytes() []byte {
	probeCarOnce.Do(func() {
		var buf bytes.Buffer
		// Create a simple CAR v1 with the probe CID as root
		carWriter, err := storage.NewWritable(&buf, []cid.Cid{ProbeCID}, car.WriteAsCarV1(true))
		if err != nil {
			// This should never happen with valid inputs
			panic(fmt.Sprintf("failed to create probe CAR writer: %v", err))
		}
		// Identity CIDs are not stored by default (no StoreIdentityCIDs option),
		// so we just finalize to get a CAR with only the header.
		// The spec says identity block MAY be skipped in the data section.
		if err = carWriter.Finalize(); err != nil {
			panic(fmt.Sprintf("failed to finalize probe CAR: %v", err))
		}
		probeCarBytes = buf.Bytes()
	})
	return probeCarBytes
}

// StreamCar streams a DAG in CARv1 format to the given writer, using the given
// selector.
func StreamCar(
	ctx context.Context,
	requestLsys linking.LinkSystem,
	out io.Writer,
	request trustlessutils.Request,
) error {
	carWriter := deferred.NewDeferredCarWriterForStream(out, []cid.Cid{request.Root}, car.AllowDuplicatePuts(request.Duplicates))
	requestLsys.StorageReadOpener = carPipe(requestLsys.StorageReadOpener, carWriter)

	cfg := traversal.Config{Root: request.Root, Selector: request.Selector()}
	lastPath, err := cfg.Traverse(ctx, requestLsys, nil)
	if err != nil {
		return err
	}

	if err := traversal.CheckPath(datamodel.ParsePath(request.Path), lastPath); err != nil {
		logger.Warn(err)
	}

	return nil
}

func carPipe(orig linking.BlockReadOpener, car *deferred.DeferredCarWriter) linking.BlockReadOpener {
	return func(lc linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
		r, err := orig(lc, lnk)
		if err != nil {
			return nil, err
		}
		byts, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		err = car.Put(lc.Ctx, lnk.(cidlink.Link).Cid.KeyString(), byts)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(byts), nil
	}
}

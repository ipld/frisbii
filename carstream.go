package frisbii

import (
	"bytes"
	"context"
	"fmt"
	"io"

	// codecs we care about
	dagpb "github.com/ipld/go-codec-dagpb"
	_ "github.com/ipld/go-ipld-prime/codec/cbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/json"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
	"github.com/ipld/go-ipld-prime/traversal/selector"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
)

var protoChooser = dagpb.AddSupportToChooser(basicnode.Chooser)

// StreamCar streams a DAG in CARv1 format to the given writer, using the given
// selector.
func StreamCar(ctx context.Context, requestLsys linking.LinkSystem, rootCid cid.Cid, selNode datamodel.Node, out io.Writer, duplicates bool) (int64, int64, error) {
	sel, err := selector.CompileSelector(selNode)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to compile selector: %w", err)
	}

	carWriter, err := carstorage.NewWritable(out, []cid.Cid{rootCid}, car.WriteAsCarV1(true), car.AllowDuplicatePuts(duplicates))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create car writer: %w", err)
	}

	erro := newErrorRecordingReadOpener(ctx, requestLsys.StorageReadOpener, carWriter)
	requestLsys.StorageReadOpener = erro.StorageReadOpener

	rootNode, err := loadNode(ctx, rootCid, requestLsys)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to load root node: %w", err)
	}

	progress := traversal.Progress{Cfg: &traversal.Config{
		Ctx:                            ctx,
		LinkSystem:                     requestLsys,
		LinkTargetNodePrototypeChooser: protoChooser,
	}}
	if err := progress.WalkAdv(rootNode, sel, visitNoop); err != nil {
		return 0, 0, fmt.Errorf("failed to complete traversal: %w", err)
	}
	if erro.err != nil {
		return 0, 0, fmt.Errorf("block load failed during traversal: %w", erro.err)
	}

	return erro.byteCount, erro.blockCount, nil
}

type errorRecordingReadOpener struct {
	ctx        context.Context
	orig       linking.BlockReadOpener
	car        carstorage.WritableCar
	err        error
	byteCount  int64
	blockCount int64
}

func newErrorRecordingReadOpener(ctx context.Context, orig linking.BlockReadOpener, car carstorage.WritableCar) *errorRecordingReadOpener {
	return &errorRecordingReadOpener{ctx, orig, car, nil, 0, 0}
}

func (erro *errorRecordingReadOpener) StorageReadOpener(lc linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
	r, err := erro.orig(lc, lnk)
	if err != nil {
		erro.err = err
		return nil, err
	}
	byts, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	err = erro.car.Put(erro.ctx, lnk.(cidlink.Link).Cid.KeyString(), byts)
	if err != nil {
		return nil, err
	}
	erro.byteCount += int64(len(byts))
	erro.blockCount++
	return bytes.NewReader(byts), nil
}

func loadNode(ctx context.Context, rootCid cid.Cid, lsys linking.LinkSystem) (datamodel.Node, error) {
	lnk := cidlink.Link{Cid: rootCid}
	lnkCtx := linking.LinkContext{Ctx: ctx}
	proto, err := protoChooser(lnk, lnkCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to choose prototype for CID %s: %w", rootCid.String(), err)
	}
	rootNode, err := lsys.Load(lnkCtx, lnk, proto)
	if err != nil {
		return nil, fmt.Errorf("failed to load root CID: %w", err)
	}
	return rootNode, nil
}

func visitNoop(p traversal.Progress, n datamodel.Node, vr traversal.VisitReason) error { return nil }

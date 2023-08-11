package frisbii

import (
	"bytes"
	"context"
	"fmt"
	"io"

	// codecs we care about
	"github.com/filecoin-project/lassie/pkg/types"
	dagpb "github.com/ipld/go-codec-dagpb"
	_ "github.com/ipld/go-ipld-prime/codec/cbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/json"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
	"github.com/ipld/go-ipld-prime/traversal/selector"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
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
func StreamCar(
	ctx context.Context,
	requestLsys linking.LinkSystem,
	rootCid cid.Cid,
	path datamodel.Path,
	dagScope types.DagScope,
	out io.Writer,
	duplicates bool,
) error {

	selNode := unixfsnode.UnixFSPathSelectorBuilder(path.String(), dagScope.TerminalSelectorSpec(), false)
	sel, err := selector.CompileSelector(selNode)
	if err != nil {
		return fmt.Errorf("failed to compile selector: %w", err)
	}

	carWriter, err := carstorage.NewWritable(out, []cid.Cid{rootCid}, car.WriteAsCarV1(true), car.AllowDuplicatePuts(duplicates))
	if err != nil {
		return fmt.Errorf("failed to create car writer: %w", err)
	}

	erro := &errorRecordingReadOpener{ctx, requestLsys.StorageReadOpener, carWriter, nil}
	requestLsys.StorageReadOpener = erro.StorageReadOpener

	rootNode, err := loadNode(ctx, rootCid, requestLsys)
	if err != nil {
		return fmt.Errorf("failed to load root node: %w", err)
	}

	progress := traversal.Progress{Cfg: &traversal.Config{
		Ctx:                            ctx,
		LinkSystem:                     requestLsys,
		LinkTargetNodePrototypeChooser: protoChooser,
	}}
	var lastPath datamodel.Path
	visitor := func(p traversal.Progress, n datamodel.Node, vr traversal.VisitReason) error {
		lastPath = p.Path
		return nil
	}
	if err := progress.WalkAdv(rootNode, sel, visitor); err != nil {
		return fmt.Errorf("failed to complete traversal: %w", err)
	}
	if erro.err != nil {
		return fmt.Errorf("block load failed during traversal: %w", erro.err)
	}
	for path.Len() > 0 {
		if lastPath.Len() == 0 {
			return fmt.Errorf("failed to traverse full path, missed: [%s]", path.String())
		}
		var seg, lastSeg datamodel.PathSegment
		seg, path = path.Shift()
		lastSeg, lastPath = lastPath.Shift()
		if seg != lastSeg {
			return fmt.Errorf("unexpected path segment visit, got [%s], expected [%s]", lastSeg.String(), seg.String())
		}
	}
	// having lastPath.Len()>0 is fine, it may be due to an "all" or "entity"
	// doing an explore-all on the remainder of the DAG after the path.

	return nil
}

type errorRecordingReadOpener struct {
	ctx  context.Context
	orig linking.BlockReadOpener
	car  carstorage.WritableCar
	err  error
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

package frisbii

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	carstorage "github.com/ipld/go-car/v2/storage"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

var protoChooser = dagpb.AddSupportToChooser(basicnode.Chooser)

func StreamCar(ctx context.Context, parentLsys linking.LinkSystem, rootCid cid.Cid, sel selector.Selector, out io.Writer) error {
	carWriter, err := carstorage.NewWritable(out, []cid.Cid{rootCid}, car.WriteAsCarV1(true), car.AllowDuplicatePuts(false))
	if err != nil {
		return fmt.Errorf("failed to create car writer: %w", err)
	}

	requestLsys := parentLsys // make a copy just for this request so we can intercept the readopener
	requestLsys.StorageReadOpener = requestStorageReadOpener(ctx, requestLsys.StorageReadOpener, carWriter)

	rootNode, err := loadNode(ctx, rootCid, requestLsys)
	if err != nil {
		return fmt.Errorf("failed to load root node: %w", err)
	}

	progress := traversal.Progress{Cfg: &traversal.Config{
		Ctx:                            ctx,
		LinkSystem:                     requestLsys,
		LinkTargetNodePrototypeChooser: protoChooser,
		// TODO: this should be safe with our basic selectors: LinkVisitOnlyOnce: true,
	}}
	if err := progress.WalkAdv(rootNode, sel, visitNoop); err != nil {
		return fmt.Errorf("failed to complete traversal: %w", err)
	}

	return nil
}

func requestStorageReadOpener(ctx context.Context, orig linking.BlockReadOpener, car carstorage.WritableCar) linking.BlockReadOpener {
	return func(lc linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
		r, err := orig(lc, lnk)
		if err != nil {
			return nil, err
		}
		byts, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		err = car.Put(ctx, lnk.(cidlink.Link).Cid.KeyString(), byts)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(byts), nil
	}
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

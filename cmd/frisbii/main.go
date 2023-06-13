package main

import (
	"context"
	"os"

	"github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/maurl"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/rvagg/go-frisbii"
	"github.com/rvagg/go-frisbii/engine"
	"github.com/urfave/cli/v2"
)

const (
	ConfigDir          = ".frisbii"
	IndexerHandlerPath = "/_ipni/"
	IndexerAnnounceUrl = "https://cid.contact/ingest/announce"
	DefaultHttpPort    = 3747
)

var logger = log.Logger("frisbii")

func main() {
	app := &cli.App{
		Name:   "frisbii",
		Usage:  "A minimal IPLD data provider for IPFS",
		Flags:  Flags,
		Action: action,
	}

	err := app.Run(os.Args)
	if err != nil {
		logger.Error(err)
		os.Exit(1)
	}
}

func action(c *cli.Context) error {
	ctx := context.Background()

	config, err := ToConfig(c)
	if err != nil {
		return err
	}

	confDir, err := configDir()
	if err != nil {
		return err
	}
	privKey, id, err := loadPrivKey(confDir)
	if err != nil {
		return err
	}

	multicar := frisbii.NewMultiCarStore(true)
	for _, carPath := range config.Cars {
		loadCar(multicar, carPath)
	}

	logWriter := c.App.Writer
	if config.LogFile != "" {
		logWriter, err = os.OpenFile(config.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
	}

	server, err := frisbii.NewFrisbiiServer(ctx, logWriter, multicar.LinkSystem(), config.Listen)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()

	frisbiiListenMaddr, err := getListenAddr(server.Addr().String(), config.PublicAddr)
	if err != nil {
		return err
	}

	logger.Infof("PeerID: %s", id.String())
	logger.Infof("Listening on %s", server.Addr())
	logger.Infof("Listening on %s", frisbiiListenMaddr.String())

	if config.Announce != AnnounceNone {
		logger.Infof("Announcing to indexer as %s", frisbiiListenMaddr.String())

		listenUrl, err := maurl.ToURL(frisbiiListenMaddr)
		if err != nil {
			return err
		}

		engine, err := engine.New(
			engine.WithPrivateKey(privKey),
			engine.WithProvider(peer.AddrInfo{ID: id, Addrs: []multiaddr.Multiaddr{frisbiiListenMaddr}}),
			engine.WithDirectAnnounce(IndexerAnnounceUrl),
			engine.WithPublisherKind(engine.HttpPublisher),
			engine.WithHttpPublisherWithoutServer(),
			engine.WithHttpPublisherHandlerPath(IndexerHandlerPath),
			engine.WithHttpPublisherListenAddr(listenUrl.Host),
			engine.WithHttpPublisherAnnounceAddr(frisbiiListenMaddr.String()),
		)
		if err != nil {
			return err
		}

		// assume announce type "roots"
		// TODO: support "all" with provider.CarMultihashIterator(idx), or similar
		engine.RegisterMultihashLister(multicar.RootsLister())

		if err := engine.Start(ctx); err != nil {
			return err
		}

		server.SetIndexerProvider(IndexerHandlerPath, engine)

		if err := server.Announce(); err != nil {
			return err
		}
	}

	select {
	case <-ctx.Done():
	case err = <-errCh:
		return err
	}
	return nil
}

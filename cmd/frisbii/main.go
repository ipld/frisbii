package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/frisbii"
	util "github.com/ipld/frisbii/internal/util"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/maurl"
	"github.com/ipni/index-provider/engine"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
	"golang.org/x/term"
)

const (
	IndexerHandlerPath = "/ipni/"
	IndexerAnnounceUrl = "https://cid.contact/ingest/announce"
	DefaultHttpPort    = 3747
)

var logger = log.Logger("frisbii")

func main() {
	// Set up a context that is canceled when the command is interrupted
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	app := &cli.App{
		Name:   "frisbii",
		Usage:  "A minimal IPLD data provider for IPFS",
		Flags:  Flags,
		Action: action,
	}

	// Set up a signal handler to cancel the context
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)
		select {
		case <-interrupt:
			cancel()
			fmt.Println("Received interrupt signal, shutting down...")
			fmt.Println("(Hit ctrl-c again to force-shutdown the daemon.)")
		case <-ctx.Done():
		}
		// Allow any further SIGTERM or SIGINT to kill process
		signal.Stop(interrupt)
	}()

	err := app.RunContext(ctx, os.Args)
	if err != nil {
		logger.Error(err)
		os.Exit(1)
	}
}

func action(c *cli.Context) error {
	ctx := c.Context

	config, err := ToConfig(c)
	if err != nil {
		return err
	}

	if config.Verbose && os.Getenv("GOLOG_LOG_LEVEL") == "" {
		_ = log.SetLogLevel("*", "DEBUG")
	}

	loader := NewLoader(c.App.ErrWriter)
	loader.SetStatus("Starting ...")
	isTerm := c.App.ErrWriter == os.Stderr && term.IsTerminal(int(os.Stderr.Fd()))
	if isTerm && !config.Verbose && os.Getenv("GOLOG_LOG_LEVEL") == "" {
		loader.Start()
		defer loader.Stop()
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigs
			loader.Stop()
			os.Exit(0)
		}()
	}

	confDir, err := util.ConfigDir()
	if err != nil {
		return err
	}

	privKey, id, err := util.LoadPrivKey(confDir)
	if err != nil {
		return err
	}

	multicar := frisbii.NewMultiReadableStorage()
	for ii, carPath := range config.Cars {
		loader.SetStatus(fmt.Sprintf("Loading CARs (%d / %d) ...", ii+1, len(config.Cars)))
		util.LoadCar(multicar, carPath)
	}

	loader.SetStatus("Loaded CARs, starting server ...")
	logWriter := c.App.Writer
	if config.LogFile != "" {
		logWriter, err = os.OpenFile(config.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
	}

	lsys := cidlink.DefaultLinkSystem()
	lsys.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)
	lsys.SetReadStorage(multicar)

	server, err := frisbii.NewFrisbiiServer(
		ctx,
		logWriter,
		lsys,
		config.Listen,
		frisbii.WithMaxResponseDuration(config.MaxResponseDuration),
		frisbii.WithMaxResponseBytes(config.MaxResponseBytes),
	)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve()
	}()

	frisbiiListenAddr, err := util.GetListenAddr(server.Addr().String(), config.PublicAddr)
	if err != nil {
		return err
	}

	logger.Infof("PeerID: %s", id.String())
	logger.Infof("Listening on %s", server.Addr())
	logger.Infof("Available as %s", frisbiiListenAddr.Url.String())
	logger.Infof("Available as %s/p2p/%s", frisbiiListenAddr.Maddr.String(), id.String())

	if config.Announce != AnnounceNone {
		if frisbiiListenAddr.Unspecified {
			return fmt.Errorf("cannot announce with unspecified listen address, use --public-addr or --listen to specify one")
		}

		loader.SetStatus("Loaded CARs, started server, announcing to indexer ...")
		logger.Infof("Announcing to indexer as %s", frisbiiListenAddr.Maddr.String())

		listenUrl, err := maurl.ToURL(frisbiiListenAddr.Maddr)
		if err != nil {
			return err
		}

		announceAddr := frisbiiListenAddr.Maddr
		ipniPath := config.IpniPath
		if strings.HasPrefix("/ipni/v1/ad/", config.IpniPath) {
			// if it's some subset of /ipni/v1/ad/ then we won't need to add it,
			// ipnisync's ServeHTTP is agnostic and only cares about the path.Base()
			// of the path.
			ipniPath = ""
		}
		if ipniPath != "" {
			httpath, err := multiaddr.NewComponent("httpath", url.PathEscape(ipniPath))
			if err != nil {
				return err
			}
			announceAddr = multiaddr.Join(announceAddr, httpath)
		}

		engine, err := engine.New(
			engine.WithPrivateKey(privKey),
			engine.WithProvider(peer.AddrInfo{ID: id, Addrs: []multiaddr.Multiaddr{frisbiiListenAddr.Maddr}}),
			engine.WithDirectAnnounce(config.AnnounceUrl.String()),
			engine.WithPublisherKind(engine.HttpPublisher),
			engine.WithHttpPublisherWithoutServer(),
			engine.WithHttpPublisherHandlerPath(ipniPath),
			engine.WithHttpPublisherListenAddr(listenUrl.Host),
			engine.WithHttpPublisherAnnounceAddr(announceAddr.String()),
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

		// use config.IpniPath here, but the adjusted ipniPath above for setting up
		// the engine; here we set our local mount expectations and it can't be
		// ""
		server.SetIndexerProvider(config.IpniPath, engine)

		if err := server.Announce(); err != nil {
			return err
		}
	}

	if loader.IsRunning() {
		loader.Stop()
		a := ""
		if config.Announce != AnnounceNone {
			a = ", announced to indexer"
		}
		laddr := "http://" + server.Addr().String()
		fmt.Fprintf(c.App.ErrWriter, " 💿 Loaded CARs, server started%s.\n", a)
		fmt.Fprintf(c.App.ErrWriter, " 💿 Frisbii thrown and ready to be fetched!\n")
		fmt.Fprintf(c.App.ErrWriter, " 💿 Listening to %s\n", laddr)
		if laddr != frisbiiListenAddr.Url.String() {
			fmt.Fprintf(c.App.ErrWriter, " 💿 Available at %s\n", frisbiiListenAddr.Url.String())
		}
		fmt.Fprintf(c.App.ErrWriter, " 💿 %s/p2p/%s\n", frisbiiListenAddr.Maddr.String(), id.String())
	}

	select {
	case <-ctx.Done():
	case err = <-errCh:
		return err
	}
	return nil
}

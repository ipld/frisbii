package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/frisbii"
	"github.com/ipld/frisbii/engine"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/maurl"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
	"golang.org/x/term"
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

	confDir, err := configDir()
	if err != nil {
		return err
	}

	privKey, id, err := loadPrivKey(confDir)
	if err != nil {
		return err
	}

	multicar := frisbii.NewMultiReadableStorage()
	for ii, carPath := range config.Cars {
		loader.SetStatus(fmt.Sprintf("Loading CARs (%d / %d) ...", ii+1, len(config.Cars)))
		loadCar(multicar, carPath)
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

	server, err := frisbii.NewFrisbiiServer(ctx, logWriter, lsys, config.MaxResponseDuration, config.MaxResponseBytes, config.Listen)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve()
	}()

	frisbiiListenAddr, err := getListenAddr(server.Addr().String(), config.PublicAddr)
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

		engine, err := engine.New(
			engine.WithPrivateKey(privKey),
			engine.WithProvider(peer.AddrInfo{ID: id, Addrs: []multiaddr.Multiaddr{frisbiiListenAddr.Maddr}}),
			engine.WithDirectAnnounce(IndexerAnnounceUrl),
			engine.WithPublisherKind(engine.HttpPublisher),
			engine.WithHttpPublisherWithoutServer(),
			engine.WithHttpPublisherHandlerPath(IndexerHandlerPath),
			engine.WithHttpPublisherListenAddr(listenUrl.Host),
			engine.WithHttpPublisherAnnounceAddr(frisbiiListenAddr.Maddr.String()),
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

type Loader struct {
	writer       io.Writer
	ticker       *time.Ticker
	isRunning    bool
	tickDuration time.Duration
	wg           sync.WaitGroup
	lk           sync.Mutex
	status       string
}

func NewLoader(w io.Writer, tickDuration ...time.Duration) *Loader {
	duration := 50 * time.Millisecond
	if len(tickDuration) > 0 && tickDuration[0] > 0 {
		duration = tickDuration[0]
	}
	return &Loader{
		writer:       w,
		tickDuration: duration,
	}
}

var winTerm = runtime.GOOS == "windows" && len(os.Getenv("WT_SESSION")) > 0

func (l *Loader) Start() {
	if !winTerm {
		fmt.Fprint(l.writer, "\033[?25l") // hide cursor
	}
	l.isRunning = true
	l.ticker = time.NewTicker(l.tickDuration)
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		spinnerChars := []string{
			// "▒▒▒▒▒▒▒▒▒▒", "█▒▒▒▒▒▒▒▒▒", "██▒▒▒▒▒▒▒▒", "███▒▒▒▒▒▒▒", "████▒▒▒▒▒▒", "████▒▒▒▒▒▒", "█████▒▒▒▒▒", "██████▒▒▒▒", "███████▒▒▒", "████████▒▒", "█████████▒",
			// "██████████", "▒█████████", "▒▒████████", "▒▒▒███████", "▒▒▒▒██████", "▒▒▒▒██████", "▒▒▒▒▒█████", "▒▒▒▒▒▒████", "▒▒▒▒▒▒▒███", "▒▒▒▒▒▒▒▒██", "▒▒▒▒▒▒▒▒▒█",
			"🌑", "🌘", "🌗", "🌖", "🌕", "🌔", "🌓", "🌒",
		}
		i := 0
		for range l.ticker.C {
			if l.isRunning {
				l.lk.Lock()
				i = i % len(spinnerChars)
				fmt.Fprintf(l.writer, "\r\033[K %s Preparing to throw Frisbii — %s", spinnerChars[i], l.status)
				i++
				l.lk.Unlock()
			} else {
				l.ticker.Stop()
				break
			}
		}
	}()
}

func (l *Loader) SetStatus(status string) {
	l.lk.Lock()
	defer l.lk.Unlock()
	l.status = status
}

func (l *Loader) Stop() {
	if l.isRunning {
		l.isRunning = false
		l.wg.Wait()
		if !winTerm {
			fmt.Fprint(l.writer, "\r\033[K\033[?25h") // show cursor
		}
	}
}

func (l *Loader) IsRunning() bool {
	return l.isRunning
}

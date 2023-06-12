package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/ipfs/go-log/v2"
	car "github.com/ipld/go-car/v2"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipni/go-libipni/maurl"
	"github.com/libp2p/go-libp2p/core/crypto"
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
	IndexerChunkSize   = 16384 // ~512k blocks with ~32b multihashes
	DefaultHttpPort    = 3747
)

var logger = log.Logger("frisbii")

func main() {
	app := &cli.App{
		Name:  "frisbii",
		Usage: "A minimal IPLD data provider for IPFS",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "car",
				Usage:    "path to CAR file to serve content from",
				Required: true,
			},
			&cli.StringFlag{
				Name:        "listen",
				Usage:       "hostname and port to listen on",
				DefaultText: "defaults to listening on all interfaces on a random port",
				Value:       ":" + strconv.FormatInt(int64(DefaultHttpPort), 10),
			},
			&cli.StringFlag{
				Name:        "announce",
				Usage:       "indexer announcement style",
				DefaultText: "defaults to not announcing content to the indexer",
				Value:       "none",
				Action: func(cctx *cli.Context, v string) error {
					switch v {
					case "none":
					case "roots":
					default:
						return errors.New("invalid announce parameter, must be of value [none,roots]")
					}
					return nil
				},
			},
			&cli.StringFlag{
				Name:  "public-addr",
				Usage: "multiaddr or URL of this server as seen by the indexer and other peers if it is different to the listen address",
			},
		},
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

	carPath := c.String("car")
	listenAddr := c.String("listen")
	announce := c.String("announce")
	publicAddr := c.String("public-addr")

	confDir, err := configDir()
	if err != nil {
		return err
	}
	privKey, id, err := loadPrivKey(confDir)
	if err != nil {
		return err
	}

	multicar := frisbii.NewMultiCarStore(true)
	loadCar(multicar, carPath)

	server, err := frisbii.NewFrisbiiServer(ctx, c.App.Writer, multicar.LinkSystem(), listenAddr)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()

	frisbiiListenMaddr, err := getListenAddr(server.Addr().String(), publicAddr)
	if err != nil {
		return err
	}

	logger.Infof("PeerID: %s", id.String())
	logger.Infof("Listening on %s", server.Addr())
	logger.Infof("Listening on %s", frisbiiListenMaddr.String())

	if announce != "none" {
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

func loadCar(multicar *frisbii.MultiCarStore, carPath string) error {
	start := time.Now()
	logger.Info("Opening CAR file...")
	carFile, err := os.Open(carPath)
	if err != nil {
		return err
	}
	store, err := carstorage.OpenReadable(carFile, car.UseWholeCIDs(false))
	if err != nil {
		return err
	}
	logger.Infof("CAR file opened in %s", time.Since(start))
	multicar.AddStore(store)
	return nil
}

func getListenAddr(serverAddr string, publicAddr string) (multiaddr.Multiaddr, error) {
	frisbiiAddr := "http://" + serverAddr
	if publicAddr != "" {
		frisbiiAddr = publicAddr
	}

	var frisbiiListenMaddr multiaddr.Multiaddr
	frisbiiUrl, err := url.Parse(frisbiiAddr)
	if err != nil {
		// try as multiaddr
		frisbiiListenMaddr, err = multiaddr.NewMultiaddr(frisbiiAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse public-addr [%s] as URL or multiaddr", frisbiiAddr)
		}
	} else {
		frisbiiListenMaddr, err = maurl.FromURL(frisbiiUrl)
		if err != nil {
			return nil, err
		}
	}

	return frisbiiListenMaddr, nil
}

func loadPrivKey(confDir string) (crypto.PrivKey, peer.ID, error) {
	// make the config dir in the user's home dir if it doesn't exist
	keyFile := path.Join(confDir, "key")
	data, err := os.ReadFile(keyFile)
	var privKey crypto.PrivKey
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, peer.ID(""), err
		}
		var err error
		privKey, _, err = crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, peer.ID(""), err
		}
		data, err := crypto.MarshalPrivateKey(privKey)
		if err != nil {
			return nil, peer.ID(""), err
		}
		if err := os.WriteFile(keyFile, data, 0600); err != nil {
			return nil, peer.ID(""), err
		}
	} else {
		var err error
		privKey, err = crypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, peer.ID(""), err
		}
	}
	id, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		return nil, peer.ID(""), err
	}
	return privKey, id, nil
}

func configDir() (string, error) {
	homedir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	confDir := path.Join(homedir, ConfigDir)
	if _, err := os.Stat(confDir); os.IsNotExist(err) {
		if err := os.Mkdir(confDir, 0700); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	}
	return confDir, nil
}

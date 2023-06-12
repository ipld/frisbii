package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"path"
	"strconv"

	"github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-unixfsnode"
	carstorage "github.com/ipld/go-car/v2/storage"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/maurl"
	provider "github.com/ipni/index-provider"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
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
						return fmt.Errorf("invalid announce parameter, must be of value [none,roots]")
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
		fmt.Println(err)
		os.Exit(1)
	}
}

func action(c *cli.Context) error {
	ctx := context.Background()

	carPath := c.String("car")
	listenAddr := c.String("listen")
	announce := c.String("announce")
	publicAddr := c.String("public-addr")

	carFile, err := os.Open(carPath)
	if err != nil {
		return err
	}

	logger.Info("Opening CAR file...")
	store, err := carstorage.OpenReadable(carFile)
	if err != nil {
		return err
	}
	logger.Info("CAR file opened")

	confDir, err := configDir()
	if err != nil {
		return err
	}

	carLsys := cidlink.DefaultLinkSystem()
	carLsys.SetReadStorage(store)
	// TODO: should we trust it? maybe only if we've generated the index or enable a --trust flag? carLsys.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&carLsys)

	server, err := frisbii.NewFrisbiiServer(ctx, c.App.Writer, carLsys, listenAddr)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()

	fmt.Fprintf(c.App.ErrWriter, "Listening on %s\n", server.Addr())

	privKey, err := loadPrivKey(confDir)
	if err != nil {
		return err
	}
	id, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.App.ErrWriter, "PeerID: %s\n", id.String())

	frisbiiAddr := "http://" + server.Addr().String()
	if publicAddr != "" {
		frisbiiAddr = publicAddr
	}

	var frisbiiListenMaddr multiaddr.Multiaddr
	frisbiiUrl, err := url.Parse(frisbiiAddr)
	if err != nil {
		// try as multiaddr
		frisbiiListenMaddr, err = multiaddr.NewMultiaddr(frisbiiAddr)
		if err != nil {
			return fmt.Errorf("failed to parse public-addr [%s] as URL or multiaddr", frisbiiAddr)
		}
	} else {
		frisbiiListenMaddr, err = maurl.FromURL(frisbiiUrl)
		if err != nil {
			return err
		}
	}
	p2p, err := multiaddr.NewComponent("p2p", id.String())
	if err != nil {
		return err
	}
	frisbiiAddrInfo := peer.AddrInfo{
		ID:    id,
		Addrs: []multiaddr.Multiaddr{frisbiiListenMaddr},
	}
	frisbiiAddrInfoP2p := peer.AddrInfo{
		ID:    id,
		Addrs: []multiaddr.Multiaddr{multiaddr.Join(frisbiiListenMaddr, p2p)},
	}
	logger.Info("Listening on ", frisbiiAddrInfoP2p.Addrs[0].String())

	if announce != "none" {
		logger.Info("Announcing to indexer as ", frisbiiListenMaddr.String())

		// assume roots
		// TODO: support "all" with provider.CarMultihashIterator(idx), or similar
		var mhLister provider.MultihashLister = func(ctx context.Context, id peer.ID, contextID []byte) (provider.MultihashIterator, error) {
			mh := make([]multihash.Multihash, 0, len(store.Roots()))
			for _, r := range store.Roots() {
				mh = append(mh, r.Hash())
			}
			fmt.Println("Announcing roots:", mh[0].B58String())
			return provider.SliceMultihashIterator(mh), nil
		}

		listenUrl, err := maurl.ToURL(frisbiiListenMaddr)
		if err != nil {
			return err
		}

		fmt.Fprintf(c.App.ErrWriter, "Announcing to indexer with PeerID %s\n", id.String())
		engine, err := engine.New(
			engine.WithPrivateKey(privKey),
			engine.WithProvider(frisbiiAddrInfoP2p),
			engine.WithDirectAnnounce(IndexerAnnounceUrl),
			engine.WithPublisherKind(engine.HttpPublisher),
			engine.WithHttpPublisherWithoutServer(),
			engine.WithHttpPublisherHandlerPath(IndexerHandlerPath),
			engine.WithHttpPublisherListenAddr(listenUrl.Host),
			engine.WithHttpPublisherAnnounceAddr(frisbiiAddrInfoP2p.Addrs[0].String()),
		)
		if err != nil {
			return err
		}
		engine.RegisterMultihashLister(mhLister)
		if err := engine.Start(ctx); err != nil {
			return err
		}
		server.SetIndexerProvider(IndexerHandlerPath, engine, &frisbiiAddrInfo)
	}

	if announce != "none" {
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

func loadPrivKey(confDir string) (crypto.PrivKey, error) {
	// make the config dir in the user's home dir if it doesn't exist
	keyFile := path.Join(confDir, "key")
	data, err := os.ReadFile(keyFile)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		k, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, err
		}
		data, err := crypto.MarshalPrivateKey(k)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(keyFile, data, 0600); err != nil {
			return nil, err
		}
		return k, nil
	}
	return crypto.UnmarshalPrivateKey(data)
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

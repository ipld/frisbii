package frisbii

import (
	"context"
	"fmt"
	"os"

	"github.com/ipfs/go-unixfsnode"
	carstorage "github.com/ipld/go-car/v2/storage"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "frisbii",
		Usage: "A basic CLI app that takes arguments",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "car",
				Usage: "Path to car file",
			},
			&cli.StringFlag{
				Name:  "listen",
				Usage: "Hostname and port to listen on",
			},
			&cli.BoolFlag{
				Name:  "announce",
				Usage: "Announce to indexer",
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
	announce := c.Bool("announce")

	if announce {
		fmt.Fprintf(c.App.ErrWriter, "indexer announcement not yet implemented\n")
	}

	carFile, err := os.Open(carPath)
	if err != nil {
		return err
	}
	store, err := carstorage.OpenReadable(carFile)
	if err != nil {
		return err
	}

	carLsys := cidlink.DefaultLinkSystem()
	carLsys.SetReadStorage(store)
	// TODO: should we trust it? maybe only if we've generated the index or enable a --trust flag? carLsys.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&carLsys)

	server, err := NewFrisbiiServer(ctx, c.App.Writer, carLsys, listenAddr)
	if err != nil {
		return err
	}

	fmt.Fprintf(c.App.ErrWriter, "Listening on %s\n", server.Addr())

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()

	select {
	case <-ctx.Done():
	case err = <-errCh:
		return err
	}
	return nil
}

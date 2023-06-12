package main

import (
	"errors"
	"path/filepath"
	"strconv"

	"github.com/urfave/cli/v2"
)

var Flags = []cli.Flag{
	&cli.StringSliceFlag{
		Name:     "car",
		Usage:    "path(s) to CAR file(s) to serve content from, can be a glob",
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
	},
	&cli.StringFlag{
		Name:  "public-addr",
		Usage: "multiaddr or URL of this server as seen by the indexer and other peers if it is different to the listen address",
	},
}

type AnnounceType string

const (
	AnnounceNone  AnnounceType = "none"
	AnnounceRoots AnnounceType = "roots"
)

type Config struct {
	Cars       []string
	Listen     string
	Announce   AnnounceType
	PublicAddr string
}

func ToConfig(c *cli.Context) (Config, error) {
	cars := c.StringSlice("car")
	carPaths := make([]string, 0)
	for _, car := range cars {
		matches, err := filepath.Glob(car)
		if err != nil {
			return Config{}, nil
		}
		carPaths = append(carPaths, matches...)
	}
	if len(carPaths) == 0 {
		return Config{}, errors.New("must specify at least one CAR file")
	}
	announceType := AnnounceNone
	announce := c.String("announce")
	switch announce {
	case "none":
	case "roots":
		announceType = AnnounceRoots
	default:
		return Config{}, errors.New("invalid announce parameter, must be of value [none,roots]")
	}

	listen := c.String("listen")
	publicAddr := c.String("public-addr")

	return Config{
		Cars:       carPaths,
		Listen:     listen,
		Announce:   announceType,
		PublicAddr: publicAddr,
	}, nil
}

package main

import (
	"errors"
	"net/url"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/urfave/cli/v2"
)

var Flags = []cli.Flag{
	&cli.StringSliceFlag{
		Name:     "car",
		Usage:    "path(s) to CAR file(s) to serve content from, can be a glob",
		Required: true,
	},
	&cli.StringFlag{
		Name:  "listen",
		Usage: "hostname and port to listen on",
		Value: ":" + strconv.FormatInt(int64(DefaultHttpPort), 10),
	},
	&cli.StringFlag{
		Name:  "announce",
		Usage: "content to announce to the indexer, one of [none,roots]",
		Value: "none",
	},
	&cli.StringFlag{
		Name:  "announce-url",
		Usage: "announcement endpoint url for the indexer",
		Value: IndexerAnnounceUrl,
	},
	&cli.StringFlag{
		Name:  "ipni-path",
		Usage: "the local path to serve IPNI content from, requests will have /ipni/v1/ad/ automatically appended to it",
		Value: IndexerHandlerPath,
	},
	&cli.StringFlag{
		Name:  "public-addr",
		Usage: "multiaddr or URL of this server as seen by the indexer and other peers if it is different to the listen address",
	},
	&cli.StringFlag{
		Name:  "log-file",
		Usage: "path to file to append HTTP request and error logs to, defaults to stdout",
	},
	&cli.DurationFlag{
		Name:  "max-response-duration",
		Usage: "maximum duration to spend responding to a request (use 0 for no limit)",
		Value: time.Minute * 5,
	},
	&cli.StringFlag{
		Name:  "max-response-bytes",
		Usage: "maximum number of bytes to send in a response (use 0 for no limit)",
		Value: "100MiB",
	},
	&cli.BoolFlag{
		Name:  "verbose",
		Usage: "enable verbose debug logging to stderr, same as setting GOLOG_LOG_LEVEL=DEBUG",
	},
}

type AnnounceType string

const (
	AnnounceNone  AnnounceType = "none"
	AnnounceRoots AnnounceType = "roots"
)

type Config struct {
	Cars                []string
	Listen              string
	Announce            AnnounceType
	AnnounceUrl         *url.URL
	IpniPath            string
	PublicAddr          string
	LogFile             string
	MaxResponseDuration time.Duration
	MaxResponseBytes    int64
	Verbose             bool
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
	announceUrl, err := url.Parse(c.String("announce-url"))
	if err != nil {
		return Config{}, err
	}

	ipniPath := c.String("ipni-path")
	listen := c.String("listen")
	publicAddr := c.String("public-addr")
	logFile := c.String("log-file")
	verbose := c.Bool("verbose")

	maxResponseDuration := c.Duration("max-response-duration")
	var maxResponseBytes uint64
	if c.String("max-response-bytes") != "0" {
		var err error
		maxResponseBytes, err = humanize.ParseBytes(c.String("max-response-bytes"))
		if err != nil {
			return Config{}, err
		}
	}

	return Config{
		Cars:                carPaths,
		Listen:              listen,
		Announce:            announceType,
		AnnounceUrl:         announceUrl,
		IpniPath:            ipniPath,
		PublicAddr:          publicAddr,
		LogFile:             logFile,
		MaxResponseDuration: maxResponseDuration,
		MaxResponseBytes:    int64(maxResponseBytes),
		Verbose:             verbose,
	}, nil
}

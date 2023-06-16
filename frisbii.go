package frisbii

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipni/go-libipni/metadata"
)

const ContextID = "frisbii"

var logger = log.Logger("frisbii")
var advMetadata = metadata.Default.New(metadata.IpfsGatewayHttp{})

// FrisbiiServer is the main server for the frisbii application, it starts an
// HTTP server to serve data according to the Trustless Gateway spec and it
// also provides a mechanism to announce the server to the indexer service.
type FrisbiiServer struct {
	ctx                 context.Context
	lsys                linking.LinkSystem
	logWriter           io.Writer
	maxResponseDuration time.Duration
	maxResponseBytes    int64

	listener        net.Listener
	mux             *http.ServeMux
	indexerProvider IndexerProvider
}

type IndexerProvider interface {
	GetPublisherHttpFunc() (http.HandlerFunc, error)
	NotifyPut(ctx context.Context, provider *peer.AddrInfo, contextID []byte, md metadata.Metadata) (cid.Cid, error)
}

func NewFrisbiiServer(
	ctx context.Context,
	logWriter io.Writer,
	lsys linking.LinkSystem,
	maxResponseDuration time.Duration,
	maxResponseBytes int64,
	address string,
) (*FrisbiiServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	return &FrisbiiServer{
		ctx:                 ctx,
		logWriter:           logWriter,
		lsys:                lsys,
		maxResponseDuration: maxResponseDuration,
		maxResponseBytes:    maxResponseBytes,

		listener: listener,
	}, nil
}

func (fs *FrisbiiServer) Addr() net.Addr {
	return fs.listener.Addr()
}

func (fs *FrisbiiServer) Serve() error {
	fs.mux = http.NewServeMux()
	fs.mux.Handle("/ipfs/", NewHttpIpfs(fs.ctx, fs.logWriter, fs.lsys, fs.maxResponseDuration, fs.maxResponseBytes))
	server := &http.Server{
		Addr:        fs.Addr().String(),
		BaseContext: func(listener net.Listener) context.Context { return fs.ctx },
		Handler:     NewLogMiddleware(fs.mux, fs.logWriter),
	}
	logger.Debugf("Serve() server on %s", fs.Addr().String())
	return server.Serve(fs.listener)
}

func (fs *FrisbiiServer) SetIndexerProvider(handlerPath string, indexerProvider IndexerProvider) error {
	fs.indexerProvider = indexerProvider
	handlerFunc, err := indexerProvider.GetPublisherHttpFunc()
	if err != nil {
		return err
	}
	fs.mux.HandleFunc(handlerPath, handlerFunc)
	logger.Debugf("SetIndexerProvider() handler on %s", handlerPath)
	return nil
}

func (fs *FrisbiiServer) Announce() error {
	if fs.indexerProvider == nil {
		return errors.New("indexer provider not setup")
	}
	if c, err := fs.indexerProvider.NotifyPut(fs.ctx, nil, []byte(ContextID), advMetadata); err != nil {
		logger.Errorf("Announce() error: %s", err)
		return err
	} else {
		logger.Debugw("Announce() complete", "advCid", c.String())
	}
	return nil
}

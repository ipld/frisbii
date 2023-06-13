package frisbii

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"

	"github.com/ipfs/go-log/v2"

	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipni/go-libipni/metadata"
	"github.com/rvagg/go-frisbii/engine"
)

const ContextID = "frisbii"

var logger = log.Logger("frisbii")

// FrisbiiServer is the main server for the frisbii application, it starts an
// HTTP server to serve data according to the Trustless Gateway spec and it
// also provides a mechanism to announce the server to the indexer service.
type FrisbiiServer struct {
	ctx             context.Context
	lsys            linking.LinkSystem
	logWriter       io.Writer
	listener        net.Listener
	mux             *http.ServeMux
	indexerProvider *engine.Engine
}

func NewFrisbiiServer(
	ctx context.Context,
	logWriter io.Writer,
	lsys linking.LinkSystem,
	address string,
) (*FrisbiiServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	return &FrisbiiServer{
		ctx:       ctx,
		lsys:      lsys,
		logWriter: logWriter,
		listener:  listener,
	}, nil
}

func (fs *FrisbiiServer) Addr() net.Addr {
	return fs.listener.Addr()
}

func (fs *FrisbiiServer) Start() error {
	fs.mux = http.NewServeMux()
	fs.mux.Handle("/ipfs/", NewHttpIpfs(fs.ctx, fs.logWriter, fs.lsys))
	server := &http.Server{
		Addr:        fs.Addr().String(),
		BaseContext: func(listener net.Listener) context.Context { return fs.ctx },
		Handler:     NewLogMiddleware(fs.mux, fs.logWriter),
	}
	logger.Debugf("Start() server on %s", fs.Addr().String())
	return server.Serve(fs.listener)
}

func (fs *FrisbiiServer) SetIndexerProvider(handlerPath string, engine *engine.Engine) error {
	fs.indexerProvider = engine
	handlerFunc, err := engine.GetPublisherHttpFunc()
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
	md := metadata.Default.New(metadata.IpfsGatewayHttp{})
	if _, err := fs.indexerProvider.NotifyPut(fs.ctx, nil, []byte(ContextID), md); err != nil {
		logger.Errorf("Announce() error: %s", err)
		return err
	}
	logger.Debugf("Announce() complete")
	return nil
}

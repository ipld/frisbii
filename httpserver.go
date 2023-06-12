package frisbii

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/rvagg/go-frisbii/engine"
)

const ContextID = "frisbii"

type FrisbiiServer struct {
	Listener net.Listener

	ctx             context.Context
	lsys            linking.LinkSystem
	mux             *http.ServeMux
	logWriter       io.Writer
	indexerProvider *engine.Engine
	announceAs      *peer.AddrInfo
}

func NewFrisbiiServer(
	ctx context.Context,
	logWriter io.Writer,
	lsys linking.LinkSystem,
	address string,
) (*FrisbiiServer, error) {

	// Listen on the given address
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	return &FrisbiiServer{
		Listener:  listener,
		ctx:       ctx,
		lsys:      lsys,
		logWriter: logWriter,
	}, nil
}

func (fs *FrisbiiServer) Addr() net.Addr {
	return fs.Listener.Addr()
}

func (fs *FrisbiiServer) Start() error {
	fs.mux = http.NewServeMux()
	fs.mux.HandleFunc("/ipfs/", handler(fs.ctx, fs.logWriter, fs.lsys))
	server := &http.Server{
		Addr:        fs.Addr().String(),
		BaseContext: func(listener net.Listener) context.Context { return fs.ctx },
		Handler:     logMiddleware(fs.mux, fs.logWriter),
	}
	return server.Serve(fs.Listener)
}

func (fs *FrisbiiServer) SetIndexerProvider(handlerPath string, engine *engine.Engine, announceAs *peer.AddrInfo) error {
	fs.indexerProvider = engine
	fs.announceAs = announceAs
	handlerFunc, err := engine.GetPublisherHttpFunc()
	if err != nil {
		return err
	}
	fmt.Println("Setting handler func for", handlerPath, "...")
	fs.mux.HandleFunc(handlerPath, handlerFunc)
	return nil
}

func (fs *FrisbiiServer) Announce() error {
	if fs.indexerProvider == nil {
		return fmt.Errorf("indexer provider not setup")
	}
	fmt.Println("Announcing with", fs.announceAs.String(), "...")
	md := metadata.Default.New(metadata.IpfsGatewayHttp{})
	c, err := fs.indexerProvider.NotifyPut(fs.ctx, fs.announceAs, []byte(ContextID), md)
	if err != nil {
		return err
	}
	fmt.Println("Announced", c.String())
	return nil
}

func handler(ctx context.Context, logWriter io.Writer, parentLsys linking.LinkSystem) http.HandlerFunc {
	return func(_res http.ResponseWriter, req *http.Request) {
		res, ok := _res.(*LoggingResponseWriter)
		if !ok {
			panic(fmt.Sprintf("expected LoggingResponseWriter, got %T", _res))
		}

		urlPath := strings.Split(req.URL.Path, "/")[1:]

		// validate CID path parameter
		cidStr := urlPath[1]
		rootCid, err := cid.Parse(cidStr)
		if err != nil {
			res.LogError(http.StatusBadRequest, fmt.Sprintf("bad CID: %s", cidStr))
			return
		}

		// Grab unixfs path if it exists
		unixfsPath := ""
		if len(urlPath) > 2 {
			unixfsPath = "/" + strings.Join(urlPath[2:], "/")
		}

		hasAccept := req.Header.Get("Accept") != ""
		acceptTypes := strings.Split(req.Header.Get("Accept"), ",")
		validAccept := false
		for _, acceptType := range acceptTypes {
			typeParts := strings.Split(acceptType, ";")
			if typeParts[0] == "*/*" || typeParts[0] == "application/*" || typeParts[0] == "application/vnd.ipld.car" {
				validAccept = true
				break
			}
		}
		hasFormat := req.URL.Query().Has("format")
		if hasAccept && !validAccept {
			res.WriteHeader(http.StatusBadRequest)
			return
		}
		if hasFormat && req.URL.Query().Get("format") != "car" {
			res.WriteHeader(http.StatusBadRequest)
			return
		}
		if !validAccept && !hasFormat {
			res.WriteHeader(http.StatusBadRequest)
			return
		}

		var filename string
		if req.URL.Query().Has("filename") {
			filename = req.URL.Query().Get("filename")
		} else {
			filename = fmt.Sprintf("%s.car", rootCid.String())
		}

		if !req.URL.Query().Has("car-scope") {
			res.LogError(http.StatusBadRequest, "missing car-scope parameter")
			return
		}

		// Parse car scope and use it to get selector
		var carScope CarScope
		switch req.URL.Query().Get("car-scope") {
		case "all":
			carScope = CarScopeAll
		case "file":
			carScope = CarScopeFile
		case "block":
			carScope = CarScopeBlock
		default:
			res.LogError(http.StatusBadRequest, fmt.Sprintf("invalid car-scope parameter: %s", req.URL.Query().Get("car-scope")))
			return
		}

		if req.URL.Query().Get("dups") == "y" { // TODO: support it, it's not hard
			res.LogError(http.StatusBadRequest, "dups=y is not currently supported")
			return
		}

		selNode := unixfsnode.UnixFSPathSelectorBuilder(unixfsPath, carScope.TerminalSelectorSpec(), false)
		sel, err := selector.CompileSelector(selNode)
		if err != nil {
			res.LogError(http.StatusInternalServerError, fmt.Sprintf("failed to compile selector from car-scope: %v", err))
			return
		}

		writer := &onFirstWriteWriter{
			w: res,
			fn: func() {
				// called once we start writing blocks into the CAR (on the first Put())
				res.Header().Set("Content-Disposition", "attachment; filename="+filename)
				res.Header().Set("Accept-Ranges", "none")
				res.Header().Set("Cache-Control", "public, max-age=29030400, immutable")
				res.Header().Set("Content-Type", "application/vnd.ipld.car; version=1")
				res.Header().Set("Etag", fmt.Sprintf("%s.car", rootCid.String()))
				res.Header().Set("X-Content-Type-Options", "nosniff")
				res.Header().Set("X-Ipfs-Path", req.URL.Path)
			},
		}

		if err := StreamCar(ctx, parentLsys, rootCid, sel, writer); err != nil {
			res.LogError(http.StatusInternalServerError, err.Error())
			return
		}
	}
}

func logMiddleware(next http.Handler, logWriter io.Writer) http.Handler {
	return http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		lres := NewLoggingResponseWriter(res, req, logWriter)
		start := time.Now()
		defer func() {
			lres.Log(http.StatusOK, time.Since(start), lres.bytes, "")
		}()
		next.ServeHTTP(lres, req)
	})
}

var _ http.ResponseWriter = (*LoggingResponseWriter)(nil)

type LoggingResponseWriter struct {
	http.ResponseWriter
	logWriter io.Writer
	req       *http.Request
	status    int
	bytes     int
}

func NewLoggingResponseWriter(w http.ResponseWriter, req *http.Request, logWriter io.Writer) *LoggingResponseWriter {
	return &LoggingResponseWriter{
		ResponseWriter: w,
		req:            req,
		logWriter:      logWriter,
	}
}

func (w *LoggingResponseWriter) Log(status int, duration time.Duration, bytes int, msg string) {
	fmt.Fprintf(
		w.logWriter,
		"%s %s %s \"%s\" %d %d %d \"%s\"\n",
		time.Now().Format(time.RFC3339),
		w.req.RemoteAddr,
		w.req.Method,
		w.req.URL,
		status,
		duration.Milliseconds(),
		bytes,
		msg,
	)
}

func (w *LoggingResponseWriter) LogError(status int, msg string) {
	w.Log(status, 0, 0, msg)
	http.Error(w.ResponseWriter, msg, status)
}

func (w *LoggingResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *LoggingResponseWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

var _ io.Writer = (*onFirstWriteWriter)(nil)

type onFirstWriteWriter struct {
	w         io.Writer
	fn        func()
	byteCount int
	once      sync.Once
}

func (w *onFirstWriteWriter) Write(p []byte) (int, error) {
	w.once.Do(w.fn)
	w.byteCount += len(p)
	return w.w.Write(p)
}

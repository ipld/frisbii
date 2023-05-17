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
)

type FrisbiiServer struct {
	ctx       context.Context
	lsys      linking.LinkSystem
	logWriter io.Writer
	listener  net.Listener
}

func (fs *FrisbiiServer) Addr() net.Addr {
	return fs.listener.Addr()
}

func (fs *FrisbiiServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ipfs/", handler(fs.ctx, fs.logWriter, fs.lsys))
	server := &http.Server{
		Addr:        fs.Addr().String(),
		BaseContext: func(listener net.Listener) context.Context { return fs.ctx },
		Handler:     mux,
	}
	return server.Serve(fs.listener)
}

func NewFrisbiiServer(ctx context.Context, logWriter io.Writer, lsys linking.LinkSystem, address string) (*FrisbiiServer, error) {
	// Listen on the given address
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

func handler(ctx context.Context, logWriter io.Writer, parentLsys linking.LinkSystem) http.HandlerFunc {
	return func(res http.ResponseWriter, req *http.Request) {
		log := func(status int, duration time.Duration, bytes int, msg string) {
			fmt.Fprintf(
				logWriter,
				"%s %s %s \"%s\" %d %d %d \"%s\"\n",
				time.Now().Format(time.RFC3339),
				req.RemoteAddr,
				req.Method,
				req.URL,
				status,
				duration.Milliseconds(),
				bytes,
				msg,
			)
		}
		logError := func(status int, msg string) {
			log(status, 0, 0, msg)
			http.Error(res, msg, status)
		}

		urlPath := strings.Split(req.URL.Path, "/")[1:]

		// validate CID path parameter
		cidStr := urlPath[1]
		rootCid, err := cid.Parse(cidStr)
		if err != nil {
			logError(http.StatusBadRequest, fmt.Sprintf("bad CID: %s", cidStr))
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
			logError(http.StatusBadRequest, "missing car-scope parameter")
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
			logError(http.StatusBadRequest, fmt.Sprintf("invalid car-scope parameter: %s", req.URL.Query().Get("car-scope")))
			return
		}

		selNode := unixfsnode.UnixFSPathSelectorBuilder(unixfsPath, carScope.TerminalSelectorSpec(), false)
		sel, err := selector.CompileSelector(selNode)
		if err != nil {
			logError(http.StatusInternalServerError, fmt.Sprintf("failed to compile selector from car-scope: %v", err))
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

		start := time.Now()
		if err := StreamCar(ctx, parentLsys, rootCid, sel, res); err != nil {
			logError(http.StatusInternalServerError, err.Error())
			return
		}
		log(http.StatusOK, time.Since(start), writer.byteCount, "")
	}
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

package frisbii

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

var _ http.Handler = (*HttpIpfs)(nil)

// HttpIpfs is an http.Handler that serves IPLD data via HTTP according to the
// Trustless Gateway specification.
type HttpIpfs struct {
	ctx        context.Context
	logWriter  io.Writer
	parentLsys linking.LinkSystem
}

func NewHttpIpfs(ctx context.Context, logWriter io.Writer, parentLsys linking.LinkSystem) *HttpIpfs {
	return &HttpIpfs{
		ctx:        ctx,
		logWriter:  logWriter,
		parentLsys: parentLsys,
	}
}

func (hi *HttpIpfs) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	logError := func(status int, msg string) {
		if lrw, ok := res.(*LoggingResponseWriter); ok {
			lrw.LogError(status, msg)
		} else {
			logger.Debug("Error handling request from [%s] for [%s] status=%d, msg=%s", req.RemoteAddr, req.URL, status, msg)
		}
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

	if req.URL.Query().Get("dups") == "y" { // TODO: support it, it's not hard
		logError(http.StatusBadRequest, "dups=y is not currently supported")
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

	if err := StreamCar(hi.ctx, hi.parentLsys, rootCid, sel, writer); err != nil {
		logError(http.StatusInternalServerError, err.Error())
		return
	}
}

package frisbii

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	lassiehttp "github.com/filecoin-project/lassie/pkg/server/http"
	lassietypes "github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
)

var _ http.Handler = (*HttpIpfs)(nil)

// HttpIpfs is an http.Handler that serves IPLD data via HTTP according to the
// Trustless Gateway specification.
type HttpIpfs struct {
	ctx       context.Context
	logWriter io.Writer
	lsys      linking.LinkSystem
}

func NewHttpIpfs(ctx context.Context, logWriter io.Writer, lsys linking.LinkSystem) *HttpIpfs {
	return &HttpIpfs{
		ctx:       ctx,
		logWriter: logWriter,
		lsys:      lsys,
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

	path := datamodel.ParsePath(req.URL.Path)
	_, path = path.Shift() // remove /ipfs

	// filter out everything but GET requests
	switch req.Method {
	case http.MethodGet:
		break
	default:
		res.Header().Add("Allow", http.MethodGet)
		logError(http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// check if CID path param is missing
	if path.Len() == 0 {
		// not a valid path to hit
		logError(http.StatusNotFound, "not found")
		return
	}

	includeDupes, err := lassiehttp.CheckFormat(req)
	if err != nil {
		logError(http.StatusBadRequest, err.Error())
		return
	}

	fileName, err := lassiehttp.ParseFilename(req)
	if err != nil {
		logError(http.StatusBadRequest, err.Error())
		return
	}

	// validate CID path parameter
	var cidSeg datamodel.PathSegment
	cidSeg, path = path.Shift()
	rootCid, err := cid.Parse(cidSeg.String())
	if err != nil {
		logError(http.StatusInternalServerError, "failed to parse CID path parameter")
		return
	}

	dagScope, err := lassiehttp.ParseScope(req)
	if err != nil {
		logError(http.StatusBadRequest, err.Error())
		return
	}

	if fileName == "" {
		fileName = fmt.Sprintf("%s%s", rootCid.String(), lassiehttp.FilenameExtCar)
	}

	selNode := unixfsnode.UnixFSPathSelectorBuilder(path.String(), dagScope.TerminalSelectorSpec(), false)

	bytesWrittenCh := make(chan struct{})
	writer := &onFirstWriteWriter{
		w: res,
		fn: func() {
			// called once we start writing blocks into the CAR (on the first Put())
			res.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fileName))
			res.Header().Set("Accept-Ranges", lassiehttp.ResponseAcceptRangesHeader)
			res.Header().Set("Cache-Control", lassiehttp.ResponseCacheControlHeader)
			res.Header().Set("Content-Type", lassiehttp.ResponseContentTypeHeader)
			res.Header().Set("Etag", etag(rootCid, path.String(), dagScope, includeDupes))
			res.Header().Set("X-Content-Type-Options", "nosniff")
			res.Header().Set("X-Ipfs-Path", "/"+datamodel.ParsePath(req.URL.Path).String())
			close(bytesWrittenCh)
		},
	}

	if err := StreamCar(hi.ctx, hi.lsys, rootCid, selNode, writer, includeDupes); err != nil {
		logError(http.StatusInternalServerError, err.Error())
		select {
		case <-bytesWrittenCh:
			logger.Debugw("unclean close", "cid", rootCid, "err", err)
			if err := closeWithUnterminatedChunk(res); err != nil {
				logger.Infow("unable to send early termination", "err", err)
			}
			return
		default:
		}
		return
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

func etag(root cid.Cid, path string, scope lassietypes.DagScope, duplicates bool) string {
	return lassietypes.RetrievalRequest{
		Cid:        root,
		Path:       path,
		Scope:      scope,
		Duplicates: duplicates,
	}.Etag()
}

// closeWithUnterminatedChunk attempts to take control of the the http conn and terminate the stream early
//
// (copied from github.com/filecoin-project/lassie/pkg/server/http/ipfs.go)
func closeWithUnterminatedChunk(res http.ResponseWriter) error {
	hijacker, ok := res.(http.Hijacker)
	if !ok {
		return errors.New("unable to access hijack interface")
	}
	conn, buf, err := hijacker.Hijack()
	if err != nil {
		return fmt.Errorf("unable to access conn through hijack interface: %w", err)
	}
	if _, err := buf.Write(lassiehttp.ResponseChunkDelimeter); err != nil {
		return fmt.Errorf("writing response chunk delimiter: %w", err)
	}
	if err := buf.Flush(); err != nil {
		return fmt.Errorf("flushing buff: %w", err)
	}
	// attempt to close just the write side
	if err := conn.Close(); err != nil {
		return fmt.Errorf("closing write conn: %w", err)
	}
	return nil
}

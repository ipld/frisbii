package frisbii

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	trustlessutils "github.com/ipld/go-trustless-utils"
	trustlesshttp "github.com/ipld/go-trustless-utils/http"
)

var _ http.Handler = (*HttpIpfs)(nil)

var (
	ErrMethodNotAllowed       = errors.New("method not allowed")
	ErrNotFound               = errors.New("not found")
	ErrInvalidCID             = errors.New("failed to parse CID path parameter")
	ErrPathNotSupportedForRaw = errors.New("path not supported for raw requests")
	ErrContentNotInCache      = errors.New("content not in cache")
)

type ErrorLogger interface {
	LogError(status int, err error)
}

// HttpIpfs is an http.Handler that serves IPLD data via HTTP according to the
// Trustless Gateway specification.
type HttpIpfs struct {
	handlerFunc http.HandlerFunc
}

type httpOptions struct {
	MaxResponseDuration time.Duration
	MaxResponseBytes    int64
	CompressionLevel    int
	LogWriter           io.Writer
	LogHandler          LogHandler
}

type HttpOption func(*httpOptions)

// WithMaxResponseDuration sets the maximum duration for a response to be
// streamed before the connection is closed. This allows a server to limit the
// amount of time a client can hold a connection open; and also restricts the
// ability to serve very large DAGs.
//
// A value of 0 will disable the limitation. This is the default.
func WithMaxResponseDuration(d time.Duration) HttpOption {
	return func(o *httpOptions) {
		o.MaxResponseDuration = d
	}
}

// WithMaxResponseBytes sets the maximum number of bytes that will be streamed
// before the connection is closed. This allows a server to limit the amount of
// data a client can request; and also restricts the ability to serve very large
// DAGs.
//
// A value of 0 will disable the limitation. This is the default.
func WithMaxResponseBytes(b int64) HttpOption {
	return func(o *httpOptions) {
		o.MaxResponseBytes = b
	}
}

// WithCompressionLevel sets the compression level for the gzip compression
// applied to the response. This allows for a trade-off between CPU and
// bandwidth. By default, the compression level is set to gzip.NoCompression;
// which means compression will be disabled.
//
// Other recommended choices are gzip.BestSpeed (1), gzip.BestCompression (9),
// and gzip.DefaultCompression (typically 6).
func WithCompressionLevel(l int) HttpOption {
	return func(o *httpOptions) {
		o.CompressionLevel = l
	}
}

// WithLogWriter sets the writer that will be used to log requests. By default,
// requests are not logged.
//
// The log format for requests (including errors) is roughly equivalent to a
// standard nginx or Apache log format; that is, a space-separated list of
// elements, where the elements that may contain spaces are quoted. The format
// of each line can be specified as:
//
//	%s %s %s "%s" %d %d %d %s "%s" "%s"
//
// Where the elements are:
//
// 1. RFC 3339 timestamp
// 2. Remote address
// 3. Method
// 4. Path
// 5. Response status code
// 6. Response duration (in milliseconds)
// 7. Response size
// 8. Compression ratio (or `-` if no compression)
// 9. User agent
// 10. Error (or `""` if no error)
func WithLogWriter(w io.Writer) HttpOption {
	return func(o *httpOptions) {
		o.LogWriter = w
	}
}

// WithLogHandler sets a handler function that will be used to log requests. By
// default, requests are not logged. This is an alternative to WithLogWriter
// that allows for more control over the logging.
func WithLogHandler(h LogHandler) HttpOption {
	return func(o *httpOptions) {
		o.LogHandler = h
	}
}

// NewHttpIpfs returns an http.Handler that serves IPLD data via HTTP according
// to the Trustless Gateway specification.
func NewHttpIpfs(
	ctx context.Context,
	lsys linking.LinkSystem,
	opts ...HttpOption,
) *HttpIpfs {
	cfg := toConfig(opts)
	handlerFunc := NewHttpIpfsHandlerFunc(ctx, lsys, opts...)
	if cfg.CompressionLevel != gzip.NoCompression {
		gzipWrapper := gziphandler.MustNewGzipLevelHandler(cfg.CompressionLevel)
		// mildly awkward level of wrapping going on here but HttpIpfs is really
		// just a HandlerFunc->Handler converter
		handlerFunc = gzipWrapper(&HttpIpfs{handlerFunc: handlerFunc}).ServeHTTP
		logger.Debugf("enabling compression with a level of %d", cfg.CompressionLevel)
	}
	return &HttpIpfs{handlerFunc: handlerFunc}
}

func toConfig(opts []HttpOption) *httpOptions {
	cfg := &httpOptions{
		CompressionLevel: gzip.NoCompression,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func (hi *HttpIpfs) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	hi.handlerFunc(res, req)
}

// isNotFoundError checks if an error represents content not being found.
// This includes format.ErrNotFound and errors that implement a NotFound() method.
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	// Check for format.ErrNotFound from go-ipld-format
	var notFound format.ErrNotFound
	if errors.As(err, &notFound) {
		return true
	}
	// Check for NotFound() interface method (used by some storage implementations)
	if nf, ok := err.(interface{ NotFound() bool }); ok && nf.NotFound() {
		return true
	}
	return false
}

func NewHttpIpfsHandlerFunc(
	ctx context.Context,
	lsys linking.LinkSystem,
	opts ...HttpOption,
) http.HandlerFunc {
	cfg := toConfig(opts)

	return func(res http.ResponseWriter, req *http.Request) {
		reqCtx := ctx
		if cfg.MaxResponseDuration > 0 {
			var cancel context.CancelFunc
			reqCtx, cancel = context.WithTimeout(ctx, cfg.MaxResponseDuration)
			defer cancel()
		}

		var rootCid cid.Cid
		bytesWrittenCh := make(chan struct{})

		logError := func(status int, err error) {
			select {
			case <-bytesWrittenCh:
				cs := "unknown"
				if rootCid.Defined() {
					cs = rootCid.String()
				}
				logger.Debugw("forcing unclean close", "cid", cs, "status", status, "err", err)
				if err := closeWithUnterminatedChunk(res); err != nil {
					log := logger.Infow
					if strings.Contains(err.Error(), "use of closed network connection") {
						log = logger.Debugw // it's just not as interesting in this case
					}
					log("unable to send early termination", "err", err)
				}
				return
			default:
				res.WriteHeader(status)
				if _, werr := res.Write([]byte(err.Error())); werr != nil {
					logger.Debugw("unable to write error to response", "err", werr)
				}
			}

			if lrw, ok := res.(ErrorLogger); ok {
				lrw.LogError(status, err)
			} else {
				logger.Debugf("error handling request from [%s] for [%s] status=%d, msg=%s", req.RemoteAddr, req.URL, status, err.Error())
			}
		}

		// filter out everything but GET and HEAD requests
		switch req.Method {
		case http.MethodGet, http.MethodHead:
			break
		default:
			res.Header().Add("Allow", http.MethodGet+", "+http.MethodHead)
			logError(http.StatusMethodNotAllowed, ErrMethodNotAllowed)
			return
		}

		path := datamodel.ParsePath(req.URL.Path)
		_, path = path.Shift() // remove /ipfs

		// check if CID path param is missing
		if path.Len() == 0 {
			// not a valid path to hit
			logError(http.StatusNotFound, ErrNotFound)
			return
		}

		// get the preferred list of  `Accept` headers if one exists; we should be
		// able to handle whatever comes back from here.
		// firsly we are looking for raw vs car, secondarily we're looking for the
		// `dups` parameter if car.
		accepts, err := trustlesshttp.CheckFormat(req)
		if err != nil {
			logError(http.StatusBadRequest, err)
			return
		}
		accept := accepts[0]

		// Parse filename parameter - trustlesshttp.ParseFilename only accepts .car
		// but frisbii also supports raw responses with .bin, so we handle that case
		fileName, err := trustlesshttp.ParseFilename(req)
		if err != nil {
			// If ParseFilename failed, check if it's because of .bin extension for raw response
			if accept.IsRaw() && req.URL.Query().Get("filename") != "" {
				fileName = req.URL.Query().Get("filename")
				if !strings.HasSuffix(fileName, ".bin") {
					logError(http.StatusBadRequest, fmt.Errorf("invalid filename parameter for raw response; expected .bin extension"))
					return
				}
			} else {
				logError(http.StatusBadRequest, err)
				return
			}
		}

		// validate CID path parameter
		var cidSeg datamodel.PathSegment
		cidSeg, path = path.Shift()
		if rootCid, err = cid.Parse(cidSeg.String()); err != nil {
			logError(http.StatusBadRequest, ErrInvalidCID)
			return
		}

		// Check for Cache-Control: only-if-cached header
		// If present and content is not found, we return 412 instead of 404
		cacheControl := req.Header.Get("Cache-Control")
		onlyIfCached := strings.Contains(strings.ToLower(cacheControl), "only-if-cached")

		var (
			dagScope  trustlessutils.DagScope   = trustlessutils.DagScopeAll
			byteRange *trustlessutils.ByteRange = nil
		)

		if accept.IsRaw() {
			if path.Len() > 0 {
				logError(http.StatusBadRequest, ErrPathNotSupportedForRaw)
				return
			}
		} else {
			accept = accept.WithMimeType(trustlesshttp.MimeTypeCar) // correct for application/* and */*

			dagScope, err = trustlesshttp.ParseScope(req)
			if err != nil {
				logError(http.StatusBadRequest, err)
				return
			}

			byteRange, err = trustlesshttp.ParseByteRange(req)
			if err != nil {
				logError(http.StatusBadRequest, err)
				return
			}
		}

		request := trustlessutils.Request{
			Root:       rootCid,
			Path:       path.String(),
			Scope:      dagScope,
			Bytes:      byteRange,
			Duplicates: accept.Duplicates,
		}

		if fileName == "" {
			ext := trustlesshttp.FilenameExtCar
			if accept.IsRaw() {
				ext = trustlesshttp.FilenameExtRaw
			}
			fileName = fmt.Sprintf("%s%s", rootCid.String(), ext)
		}

		setHeaders := func() {
			// called once we start writing blocks into the CAR (on the first Put())

			close(bytesWrittenCh) // signal that we've started writing, so we can't log errors to the response now

			if accept.IsCar() {
				res.Header().Set("Accept-Ranges", "none")
			}
			res.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fileName))
			res.Header().Set("Cache-Control", trustlesshttp.ResponseCacheControlHeader)
			res.Header().Set("Content-Type", accept.WithQuality(1).String())

			// Set Content-Location if format was negotiated via Accept header
			// (helps HTTP caches store formats separately)
			requestPath := req.URL.Path
			if req.URL.RawQuery != "" {
				requestPath += "?" + req.URL.RawQuery
			}
			if contentLoc := accept.ContentLocation(requestPath); contentLoc != "" {
				res.Header().Set("Content-Location", contentLoc)
			}

			// Set X-Ipfs-Roots for simple CID requests
			// (omitted for path requests in streaming gateways)
			if roots := request.IpfsRoots(); roots != "" {
				res.Header().Set("X-Ipfs-Roots", roots)
			}

			etag := request.Etag("dfs") // Frisbii only supports DFS ordering
			switch res.(type) {
			case *gziphandler.GzipResponseWriter, gziphandler.GzipResponseWriterWithCloseNotify:
				// there are conditions where we may have a GzipResponseWriter but the
				// response will not be compressed, but they are related to very small
				// response sizes so this shouldn't matter (much)
				etag = etag[:len(etag)-1] + ".gz\""
			}
			res.Header().Set("Etag", etag)
			res.Header().Set("X-Content-Type-Options", "nosniff")
			res.Header().Set("X-Ipfs-Path", "/"+datamodel.ParsePath(req.URL.Path).String())
			res.Header().Set("Vary", "Accept, Accept-Encoding")
		}

		var writer io.Writer = newIpfsResponseWriter(res, cfg.MaxResponseBytes, setHeaders)

		if lrw, ok := res.(*LoggingResponseWriter); ok {
			writer = &countingWriter{writer, lrw}
		} else if grw, ok := res.(*gziphandler.GzipResponseWriter); ok {
			if lrw, ok := grw.ResponseWriter.(*LoggingResponseWriter); ok {
				writer = &countingWriter{writer, lrw}
			}
		}

		// For HEAD requests, we only set headers, no body
		isHeadRequest := req.Method == http.MethodHead

		// Special handling for probe CID
		if rootCid.Equals(ProbeCID) {
			// Probe path handling - special identity CID with empty content
			if isHeadRequest {
				// For HEAD, just set headers without body
				setHeaders()
			} else if accept.IsRaw() {
				// For raw format, return empty body (identity CID has no content)
				// Write empty response for GET (identity CID has empty content)
				_, _ = writer.Write([]byte{})
			} else {
				// For CAR format, write the pre-generated probe CAR bytes
				if _, err := writer.Write(getProbeCarBytes()); err != nil {
					logger.Debugw("probe CID CAR streaming error", "cid", rootCid, "err", err)
					logError(http.StatusInternalServerError, err)
				}
			}
		} else if isHeadRequest {
			// HEAD request - verify content exists and set headers, but don't send body
			// For both raw and CAR formats, we verify by checking if the root block exists
			if _, err := lsys.LoadRaw(linking.LinkContext{Ctx: reqCtx}, cidlink.Link{Cid: rootCid}); err != nil {
				if isNotFoundError(err) {
					if onlyIfCached {
						logError(http.StatusPreconditionFailed, ErrContentNotInCache)
					} else {
						logError(http.StatusNotFound, err)
					}
				} else {
					logError(http.StatusInternalServerError, err)
				}
			} else {
				// Content exists, set headers without body
				setHeaders()
			}
		} else if accept.IsRaw() {
			// GET request for raw block - send the actual block
			if byts, err := lsys.LoadRaw(linking.LinkContext{Ctx: reqCtx}, cidlink.Link{Cid: rootCid}); err != nil {
				if isNotFoundError(err) {
					if onlyIfCached {
						logError(http.StatusPreconditionFailed, ErrContentNotInCache)
					} else {
						logError(http.StatusNotFound, err)
					}
				} else {
					logError(http.StatusInternalServerError, err)
				}
			} else if _, err := writer.Write(byts); err != nil {
				logError(http.StatusInternalServerError, err)
			}
		} else {
			// GET request for CAR - stream the CAR
			if err := StreamCar(reqCtx, lsys, writer, request); err != nil {
				logger.Debugw("error streaming CAR", "cid", rootCid, "err", err)
				if isNotFoundError(err) {
					if onlyIfCached {
						logError(http.StatusPreconditionFailed, ErrContentNotInCache)
					} else {
						logError(http.StatusNotFound, err)
					}
				} else {
					logError(http.StatusInternalServerError, err)
				}
			}
		}
	}
}

var _ io.Writer = (*countingWriter)(nil)

type countingWriter struct {
	io.Writer
	lrw *LoggingResponseWriter
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.Writer.Write(p)
	cw.lrw.WroteBytes(n)
	return n, err
}

var _ io.Writer = (*ipfsResponseWriter)(nil)

type ipfsResponseWriter struct {
	w         io.Writer
	fn        func()
	byteCount int
	once      sync.Once
	maxBytes  int64
}

func newIpfsResponseWriter(w io.Writer, maxBytes int64, fn func()) *ipfsResponseWriter {
	return &ipfsResponseWriter{
		w:        w,
		maxBytes: maxBytes,
		fn:       fn,
	}
}

func (w *ipfsResponseWriter) Write(p []byte) (int, error) {
	w.once.Do(w.fn)
	w.byteCount += len(p)
	if w.maxBytes > 0 && int64(w.byteCount) > w.maxBytes {
		return 0, fmt.Errorf("response too large: %d bytes", w.byteCount)
	}
	return w.w.Write(p)
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
	if _, err := buf.Write(trustlesshttp.ResponseChunkDelimeter); err != nil {
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

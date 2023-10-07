package frisbii

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var _ http.Handler = (*LogMiddleware)(nil)
var _ ErrorLogger = (*LoggingResponseWriter)(nil)

type LogHandler func(
	time time.Time,
	remoteAddr string,
	method string,
	url url.URL,
	status int,
	duration time.Duration,
	bytes int,
	compressionRatio string,
	userAgent string,
	msg string,
)

// LogMiddlware is a middleware that logs requests to the given io.Writer.
// it wraps requests in a LoggingResponseWriter that can be used to log
// standardised messages to the writer.
type LogMiddleware struct {
	next       http.Handler
	logWriter  io.Writer
	logHandler LogHandler
}

// NewLogMiddleware creates a new LogMiddleware to insert into an HTTP call
// chain.
//
// The WithLogWriter option can be used to set the writer to log to.
//
// The WithLogHandler option can be used to set a custom log handler.
func NewLogMiddleware(next http.Handler, httpOptions ...HttpOption) *LogMiddleware {
	cfg := toConfig(httpOptions)
	return &LogMiddleware{
		next:       next,
		logWriter:  cfg.LogWriter,
		logHandler: cfg.LogHandler,
	}
}

func (lm *LogMiddleware) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	if lm.logHandler != nil || lm.logWriter != nil {
		lres := NewLoggingResponseWriter(res, req, lm.logWriter, lm.logHandler)
		start := time.Now()
		defer func() {
			lres.Log(lres.status, start, lres.sentBytes, lres.CompressionRatio(), "")
		}()
		res = lres
	}
	lm.next.ServeHTTP(res, req)
}

var _ http.ResponseWriter = (*LoggingResponseWriter)(nil)

type LoggingResponseWriter struct {
	http.ResponseWriter
	logWriter  io.Writer
	logHandler LogHandler
	req        *http.Request
	status     int
	wroteBytes int
	sentBytes  int
	wrote      bool
}

// NewLoggingResponseWriter creates a new LoggingResponseWriter that is used
// on a per-request basis to log information about the request.
func NewLoggingResponseWriter(
	w http.ResponseWriter,
	req *http.Request,
	logWriter io.Writer,
	logHandler LogHandler,
) *LoggingResponseWriter {
	return &LoggingResponseWriter{
		ResponseWriter: w,
		req:            req,
		logWriter:      logWriter,
		logHandler:     logHandler,
	}
}

func (w *LoggingResponseWriter) CompressionRatio() string {
	if w.sentBytes == 0 || w.wroteBytes == 0 || w.wroteBytes == w.sentBytes {
		return "-"
	}
	s := fmt.Sprintf("%.2f", float64(w.wroteBytes)/float64(w.sentBytes))
	if s == "0.00" {
		return "-"
	}
	return s
}

func (w *LoggingResponseWriter) Log(
	status int,
	start time.Time,
	bytes int,
	CompressionRatio string,
	msg string,
) {
	if w.wrote {
		return
	}
	duration := time.Since(start)
	w.wrote = true
	remoteAddr := w.req.RemoteAddr
	if ss := strings.Split(remoteAddr, ":"); len(ss) > 0 {
		remoteAddr = ss[0]
	}
	if w.logWriter != nil {
		fmt.Fprintf(
			w.logWriter,
			"%s %s %s \"%s\" %d %d %d %s %s %s\n",
			start.Format(time.RFC3339),
			remoteAddr,
			w.req.Method,
			w.req.URL,
			status,
			duration.Milliseconds(),
			bytes,
			CompressionRatio,
			strconv.Quote(w.req.UserAgent()),
			strconv.Quote(msg),
		)
	}
	if w.logHandler != nil {
		w.logHandler(
			start,
			remoteAddr,
			w.req.Method,
			*w.req.URL,
			status,
			duration,
			bytes,
			CompressionRatio,
			strconv.Quote(w.req.UserAgent()),
			strconv.Quote(msg),
		)
	}
}

func (w *LoggingResponseWriter) LogError(status int, err error) {
	msg := err.Error()
	// unwrap error and find the msg at the bottom error
	for {
		if e := errors.Unwrap(err); e != nil {
			msg = e.Error()
			err = e
		} else {
			break
		}
	}
	w.Log(status, time.Now(), 0, "-", msg)
	w.status = status
	if w.sentBytes == 0 {
		http.Error(w.ResponseWriter, strconv.Quote(msg), status)
	}
}

func (w *LoggingResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *LoggingResponseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.sentBytes += n
	return n, err
}

func (w *LoggingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, errors.New("http.Hijacker not implemented")
}

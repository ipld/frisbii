package frisbii_test

import (
	"compress/gzip"
	"context"
	"io"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	unixfstestutil "github.com/ipfs/go-unixfsnode/testutil"
	"github.com/ipld/frisbii"
	"github.com/ipld/go-car/v2"
	unixfsgen "github.com/ipld/go-fixtureplate/generator"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-trustless-utils/testutil"
	"github.com/stretchr/testify/require"
)

func TestFrisbiiServer(t *testing.T) {
	testCases := []struct {
		name                   string
		acceptGzip             bool
		noClientCompression    bool
		serverCompressionLevel int
		expectGzip             bool
	}{
		{
			name: "default",
		},
		{
			name:                   "no client compression (no server gzip)",
			noClientCompression:    true,
			serverCompressionLevel: gzip.NoCompression,
			expectGzip:             false,
		},
		{
			name:                   "no client compression (with server gzip)",
			noClientCompression:    true,
			serverCompressionLevel: gzip.DefaultCompression,
			expectGzip:             false,
		},
		{
			name:                   "gzip (with server 1)",
			acceptGzip:             true,
			serverCompressionLevel: gzip.BestSpeed,
			expectGzip:             true,
		},
		{
			name:                   "gzip (with server 9)",
			acceptGzip:             true,
			serverCompressionLevel: gzip.BestCompression,
			expectGzip:             true,
		},
		{
			name:                   "gzip (no server gzip)",
			acceptGzip:             true,
			serverCompressionLevel: gzip.NoCompression,
			expectGzip:             false,
		},
		{
			name:                   "gzip transparent (no server gzip)",
			serverCompressionLevel: gzip.NoCompression,
			expectGzip:             false,
		},
		{
			name:                   "gzip transparent (with server gzip)",
			serverCompressionLevel: gzip.DefaultCompression,
			expectGzip:             true,
		},
	}

	rndSeed := time.Now().UTC().UnixNano()
	t.Logf("random seed: %d", rndSeed)
	var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

	store := &testutil.CorrectedMemStore{ParentStore: &memstore.Store{Bag: make(map[string][]byte)}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	lsys.TrustedStorage = true

	entity, err := unixfsgen.Parse("file:1MiB")
	require.NoError(t, err)
	t.Logf("Generating: %s", entity.Describe(""))
	rootEnt, err := entity.Generate(lsys, rndReader)
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			opts := []frisbii.HttpOption{}
			if tc.serverCompressionLevel != gzip.NoCompression {
				opts = append(opts, frisbii.WithCompressionLevel(tc.serverCompressionLevel))
			}
			server, err := frisbii.NewFrisbiiServer(ctx, nil, lsys, "localhost:0", opts...)
			req.NoError(err)
			go func() {
				req.NoError(server.Serve())
			}()
			addr := server.Addr()

			request, err := http.NewRequest("GET", "http://"+addr.String()+"/ipfs/"+rootEnt.Root.String(), nil)
			request.Header.Set("Accept", "application/vnd.ipld.car")
			if tc.acceptGzip {
				request.Header.Set("Accept-Encoding", "gzip")
			}
			req.NoError(err)
			request = request.WithContext(ctx)
			client := &http.Client{Transport: &http.Transport{DisableCompression: tc.noClientCompression}}
			response, err := client.Do(request)
			req.NoError(err)
			if response.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(response.Body)
				req.Failf("wrong response code not received", "expected %d, got %d; body: [%s]", http.StatusOK, response.StatusCode, string(body))
			}

			req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=y", response.Header.Get("Content-Type"))
			req.Equal("Accept, Accept-Encoding", response.Header.Get("Vary"))

			rdr := response.Body
			if tc.expectGzip {
				if tc.noClientCompression || tc.acceptGzip { // in either of these cases we expect to handle it ourselves
					req.Equal("gzip", response.Header.Get("Content-Encoding"))
					rdr, err = gzip.NewReader(response.Body)
					req.NoError(err)
				} // else should be handled by the go client
				req.Regexp(`\.car\.\w{12,13}\.gz"$`, response.Header.Get("Etag"))
			} else {
				req.Regexp(`\.car\.\w{12,13}"$`, response.Header.Get("Etag"))
			}
			cr, err := car.NewBlockReader(rdr)
			req.NoError(err)
			req.Equal(cr.Version, uint64(1))
			req.Equal(cr.Roots, []cid.Cid{rootEnt.Root})

			wantCids := toCids(rootEnt)
			gotCids := make([]cid.Cid, 0)
			for {
				blk, err := cr.Next()
				if err != nil {
					req.ErrorIs(err, io.EOF)
					break
				}
				req.NoError(err)
				gotCids = append(gotCids, blk.Cid())
			}
			req.ElementsMatch(wantCids, gotCids)
		})
	}
}

func toCids(e unixfstestutil.DirEntry) []cid.Cid {
	cids := make([]cid.Cid, 0)
	var r func(e unixfstestutil.DirEntry)
	r = func(e unixfstestutil.DirEntry) {
		cids = append(cids, e.SelfCids...)
		for _, c := range e.Children {
			r(c)
		}
	}
	r(e)
	return cids
}

// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServerTimeoutsWedgeKillBlockedWrite pins the fix: a response write
// blocked on a peer that stops reading (a zero-window wedge) must be
// reclaimed by the server-wide WriteTimeout instead of orphaning the
// connection forever. With setupServerTimeouts applied the handler's Write
// returns os.ErrDeadlineExceeded and the connection is closed within
// WriteTimeout + slack. The applied WriteTimeout is shortened below to keep
// the suite fast, so this test pins the reclaim mechanism; that the helper
// actually sets the field is pinned by TestSetupServerTimeoutsFieldValues.
func TestServerTimeoutsWedgeKillBlockedWrite(t *testing.T) {
	// Short test WriteTimeout mirrors the helper's field shape (not the prod
	// 60s) to keep the suite fast. The mechanism under test is identical.
	const testWriteTimeout = 500 * time.Millisecond
	const slack = 3 * time.Second

	writeErrCh := make(chan error, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		// A multi-MB body: once the client stops reading and the send buffer
		// fills, this Write blocks and the write deadline must reclaim it.
		payload := make([]byte, 32<<20)
		_, err := w.Write(payload)
		writeErrCh <- err
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := &http.Server{Handler: handler}
	setupServerTimeouts(srv) // GREEN: the fix under test.
	// Assert before overwriting: shortening the field for speed would otherwise
	// mask a helper that never set it, leaving this test green against exactly
	// the regression it exists to catch.
	require.NotZero(t, srv.WriteTimeout, "setupServerTimeouts must set WriteTimeout")
	srv.WriteTimeout = testWriteTimeout // shorten the applied field for speed.
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n"))
	require.NoError(t, err)

	// Read only the first bytes, then stop reading (do NOT close) so the
	// server-side Write blocks on a full send buffer and the write deadline,
	// anchored at request-header read, fires.
	firstByte := make([]byte, 1024)
	_, err = conn.Read(firstByte)
	require.NoError(t, err)

	select {
	case werr := <-writeErrCh:
		require.Error(t, werr, "handler Write unexpectedly succeeded")
		assert.ErrorIs(t, werr, os.ErrDeadlineExceeded,
			"wedged handler Write should fail with a deadline error, got: %v", werr)
	case <-time.After(testWriteTimeout + slack):
		t.Fatalf("handler Write did not return within WriteTimeout+slack (%s); "+
			"the wedged write was never bounded", testWriteTimeout+slack)
	}

	// The server closes the wedged connection: a subsequent read drains the
	// buffered bytes then observes the close (EOF/reset) within slack. A
	// deadline-exceeded here would mean the conn was never reclaimed.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(slack)))
	drain := make([]byte, 4096)
	for {
		_, rerr := conn.Read(drain)
		if rerr != nil {
			assert.NotErrorIs(t, rerr, os.ErrDeadlineExceeded,
				"server did not close the wedged connection within slack")
			break
		}
	}
}

// TestServerTimeoutsIdleOrphanKill pins the idle-connection half of the fix: a
// client that completes a request, stops reading, and holds the keepalive
// connection open without issuing another request must have that connection
// reclaimed by IdleTimeout. Against a bare no-timeout server the connection is
// never closed and the read below trips its deadline (red).
func TestServerTimeoutsIdleOrphanKill(t *testing.T) {
	const testIdleTimeout = 500 * time.Millisecond
	const slack = 3 * time.Second

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		// Small response: fits the kernel send buffer, handler returns cleanly,
		// leaving an idle keepalive connection.
		_, _ = io.WriteString(w, "ok")
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := &http.Server{Handler: handler}
	setupServerTimeouts(srv) // GREEN: the fix under test.
	// Assert before overwriting — see the note in the wedge-kill test.
	require.NotZero(t, srv.IdleTimeout, "setupServerTimeouts must set IdleTimeout")
	srv.IdleTimeout = testIdleTimeout // shorten the applied field for speed.
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n"))
	require.NoError(t, err)

	// Consume the full response so the connection becomes idle (keepalive).
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	require.NoError(t, err)
	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	idleStart := time.Now()

	// Hold the connection open without another request. The server must close
	// it within IdleTimeout + slack; the client observes the close as io.EOF.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(testIdleTimeout+slack)))
	_, err = br.Read(make([]byte, 1))
	elapsed := time.Since(idleStart)

	assert.ErrorIs(t, err, io.EOF,
		"server should close the idle keepalive connection (got %v after %s)", err, elapsed)
	assert.Less(t, elapsed, testIdleTimeout+slack,
		"idle connection was not reclaimed within IdleTimeout+slack")
}

// TestSetupServerTimeoutsFieldValues pins the deliberate field decisions with
// no timed wait: ReadTimeout stays 0 on purpose (request
// bodies here are small; slow-loris is covered by ReadHeaderTimeout; SSE
// overrides WriteTimeout per-response) while the other three bound the wedge.
func TestSetupServerTimeoutsFieldValues(t *testing.T) {
	t.Parallel()

	var srv http.Server
	setupServerTimeouts(&srv)

	assert.Equal(t, time.Duration(0), srv.ReadTimeout,
		"ReadTimeout must stay 0 (deliberate: small bodies, gRPC log upload is separate)")
	assert.Equal(t, 10*time.Second, srv.ReadHeaderTimeout)
	assert.Equal(t, 60*time.Second, srv.WriteTimeout)
	assert.Equal(t, 120*time.Second, srv.IdleTimeout)
}

// wantServerConstructions is how many http.Server literals this package builds:
// run() builds four across its two branches. Adding or removing a server is a
// deliberate act, so updating this number is part of it.
const wantServerConstructions = 4

// TestEveryHTTPServerGetsTimeouts pins the WIRING: every http.Server this
// package constructs must be handed to setupServerTimeouts. A helper that sets
// the right fields is worthless at a construction site that forgets to call it,
// and dropping one call is the realistic regression — run() builds four servers
// across two branches, and this file is a fork of upstream, so run() is rebased
// against upstream changes and a call is easy to lose in a conflict resolution.
//
// The run() function cannot be invoked from a test: it binds listeners, opens a
// store, and blocks. So the invariant is checked where it actually lives — in
// the source.
// The package is parsed and every composite literal of type http.Server is
// matched against the setupServerTimeouts arguments found in the same function.
// A new server added without the call fails here, naming the line.
//
// This is a stopgap for a structural gap: with each construction site extracted
// into a named constructor that ends in setupServerTimeouts, the same invariant
// would be a plain table test over those constructors' return values, and this
// AST walk could go away.
func TestEveryHTTPServerGetsTimeouts(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool { //nolint:staticcheck // ParseDir is adequate for this build-tag-agnostic AST-only walk over the package's non-test sources
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	require.NoError(t, err)
	require.NotEmpty(t, pkgs, "no non-test sources parsed; the walk below would vacuously pass")

	// Per enclosing function: where http.Server literals are built, and which
	// identifiers were passed to setupServerTimeouts.
	type siteSet struct {
		built   map[string]token.Position // variable name -> literal position
		covered map[string]struct{}       // names passed to setupServerTimeouts
	}

	var totalBuilt int
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				// setupServerTimeouts itself takes the server as a parameter;
				// it is the definition of the contract, not a call site.
				if fn.Name.Name == "setupServerTimeouts" {
					continue
				}

				sites := siteSet{
					built:   map[string]token.Position{},
					covered: map[string]struct{}{},
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					switch node := n.(type) {
					case *ast.AssignStmt:
						for i, rhs := range node.Rhs {
							if !isHTTPServerLiteral(rhs) || i >= len(node.Lhs) {
								continue
							}
							name, ok := node.Lhs[i].(*ast.Ident)
							if !ok {
								continue
							}
							sites.built[name.Name] = fset.Position(rhs.Pos())
						}
					case *ast.ValueSpec:
						// `var x = &http.Server{…}`. ast.Inspect descends into
						// DeclStmt, so this needs no separate walk — but without
						// this case the declaration form is invisible and a
						// server built that way is silently unchecked.
						for i, val := range node.Values {
							if !isHTTPServerLiteral(val) || i >= len(node.Names) {
								continue
							}
							sites.built[node.Names[i].Name] = fset.Position(val.Pos())
						}
					case *ast.CallExpr:
						id, ok := node.Fun.(*ast.Ident)
						if !ok || id.Name != "setupServerTimeouts" || len(node.Args) != 1 {
							return true
						}
						if arg, ok := node.Args[0].(*ast.Ident); ok {
							sites.covered[arg.Name] = struct{}{}
						}
					}
					return true
				})

				for name, pos := range sites.built {
					totalBuilt++
					_, ok := sites.covered[name]
					assert.True(t, ok,
						"%s: http.Server %q is constructed in %s() without a "+
							"setupServerTimeouts call — its connections are unbounded",
						pos, name, fn.Name.Name)
				}
			}
		}
	}

	// Guard the walk itself. NotZero is too weak: it still passes if the walk
	// silently stops seeing three of the four sites — which is exactly what a
	// construction written in a form the walk does not match looks like. Pin
	// the count, so losing a site to an unmatched form fails here instead of
	// passing over less code than it did yesterday.
	assert.Equal(t, wantServerConstructions, totalBuilt,
		"expected %d http.Server constructions in this package, walked %d — "+
			"either a server was added or removed (update this count), or one is "+
			"written in a form the walk does not match and is now unchecked",
		wantServerConstructions, totalBuilt)
}

// isHTTPServerLiteral reports whether expr builds an http.Server, as either
// &http.Server{…} or http.Server{…}.
func isHTTPServerLiteral(expr ast.Expr) bool {
	if unary, ok := expr.(*ast.UnaryExpr); ok && unary.Op == token.AND {
		expr = unary.X
	}
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return false
	}
	sel, ok := lit.Type.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Server" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "http"
}

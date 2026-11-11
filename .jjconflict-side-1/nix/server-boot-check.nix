# Boots the built woodpecker-server and requires /healthz and the embedded UI
# index to answer. A server that compiles but cannot boot (e.g. an empty
# web/dist embed) fails here instead of on the first host that runs it.
{
  pkgs,
  woodpecker-server,
}:
pkgs.runCommand "woodpecker-server-boot-check"
  {
    nativeBuildInputs = [ pkgs.curl ];
  }
  ''
    export HOME="$TMPDIR"
    log="$TMPDIR/server.log"
    WOODPECKER_HOST=http://localhost \
    WOODPECKER_SERVER_ADDR=127.0.0.1:19000 \
    WOODPECKER_GRPC_ADDR=127.0.0.1:19001 \
    WOODPECKER_DATABASE_DRIVER=sqlite3 \
    WOODPECKER_DATABASE_DATASOURCE="$TMPDIR/woodpecker.sqlite" \
    WOODPECKER_AGENT_SECRET=boot-check \
    WOODPECKER_GITEA=true \
    WOODPECKER_FORGE_URL=http://127.0.0.1:9 \
    WOODPECKER_FORGE_OAUTH_CLIENT=boot-check \
    WOODPECKER_FORGE_OAUTH_SECRET=boot-check \
      ${pkgs.lib.getExe woodpecker-server} >"$log" 2>&1 &
    pid=$!

    ok=0
    deadline=$((SECONDS + 30))
    while [ "$SECONDS" -lt "$deadline" ]; do
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      if curl -fs --max-time 2 -o /dev/null http://127.0.0.1:19000/healthz; then
        ok=1
        break
      fi
      sleep 0.25
    done

    if [ "$ok" != 1 ]; then
      echo "woodpecker-server did not serve /healthz:" >&2
      cat "$log" >&2
      exit 1
    fi
    if ! curl -fsS --max-time 10 -o "$TMPDIR/index.html" http://127.0.0.1:19000/; then
      echo "woodpecker-server did not serve the UI index:" >&2
      cat "$log" >&2
      exit 1
    fi
    grep -q '<div id="app"' "$TMPDIR/index.html" || {
      echo "UI index is not the built web UI:" >&2
      head -c 400 "$TMPDIR/index.html" >&2
      exit 1
    }

    kill "$pid"
    wait "$pid" || true
    touch "$out"
  ''

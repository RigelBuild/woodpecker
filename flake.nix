{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=master";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # Fork build version. Must not collide with upstream tags (`v3.16.x`)
        # or upstream dev builds (`next-<sha>`). Bump the `-rigel.N` suffix per
        # fork release.
        version = "3.17.0-rigel.2";

        # The repo requires Go 1.26 (go.mod toolchain); pin it so the sandboxed
        # build never tries to download a toolchain.
        buildGoModule = pkgs.buildGoModule.override { go = pkgs.go_1_26; };

        ldflags = [
          "-s"
          "-w"
          "-X go.woodpecker-ci.org/woodpecker/v3/version.Version=${version}"
        ];

        # Match the upstream/nixpkgs binary layout: bin/woodpecker-<cmd>.
        postInstall = ''
          for f in "$out"/bin/*; do
            mv -- "$f" "$(dirname "$f")/woodpecker-$(basename "$f")"
          done
        '';

        # Locked against this fork's go.sum / web pnpm-lock. To refresh: set to
        # pkgs.lib.fakeHash, build, and paste the hash nix reports.
        vendorHash = "sha256-BLqkVqMadojMeL27L3YbRn2rmgtqixZidrpbhmE63lg=";
        webuiHash = "sha256-N6XjylPpJu5VCyp/j71pog71q/yvShOoklsv6/kgKBo=";

        # Re-rooted to a content-addressed copy of this fork's own subtree
        # (SEA-1860): `self.outPath` is a subpath into the whole-repo store
        # object, so coercing it (`src = self`) makes the server/agent/webui
        # depend on the whole-repo store path — a rebuild on every monorepo
        # commit. `builtins.path` re-copies only this subtree into a
        # content-addressed path, so a docs-only merge is a no-op. The FOD
        # hashes (vendorHash/webuiHash) are unaffected — they hash fetched
        # deps, not the src store-path name.
        src = builtins.path {
          path = self.outPath;
          name = "woodpecker-source";
        };

        # The Vue web UI, built from this fork's web/ so future UI changes are
        # picked up (not reused from upstream).
        woodpecker-webui = pkgs.stdenv.mkDerivation (finalAttrs: {
          pname = "woodpecker-webui";
          inherit version;
          src = "${src}/web";

          pnpmDeps = pkgs.fetchPnpmDeps {
            inherit (finalAttrs) pname version src;
            pnpm = pkgs.pnpm_10;
            fetcherVersion = 3;
            hash = webuiHash;
          };

          nativeBuildInputs = [
            pkgs.nodejs
            pkgs.pnpmConfigHook
            pkgs.pnpm_10
          ];

          buildPhase = ''
            runHook preBuild
            pnpm build
            runHook postBuild
          '';

          installPhase = ''
            runHook preInstall
            cp -r dist $out
            runHook postInstall
          '';
        });

        woodpecker-server = buildGoModule {
          pname = "woodpecker-server";
          inherit version ldflags postInstall vendorHash src;

          subPackages = "cmd/server";
          env.CGO_ENABLED = 1;

          # The server embeds the web UI (//go:embed all:dist/*); stage the
          # built UI before the Go build. `web/dist/` is a tracked path in the
          # source (a .gitkeep anchors the go:embed target so a clean clone
          # compiles), so a plain `cp -r SRC web/dist` would copy INTO the
          # existing dir (dist/<store-name>/index.html) and leave
          # dist/index.html absent -> the server dies at boot on "cannot find
          # index.html". Remove the anchored dir first, and copy with -T
          # (--no-target-directory) so DEST is always the target and the copy
          # can never nest even if the dir reappears.
          postPatch = ''
            rm -rf web/dist
            cp -rT ${woodpecker-webui} web/dist
          '';

          meta.mainProgram = "woodpecker-server";
        };

        woodpecker-agent = buildGoModule {
          pname = "woodpecker-agent";
          inherit version ldflags postInstall vendorHash src;

          subPackages = "cmd/agent";
          env.CGO_ENABLED = 0;

          meta.mainProgram = "woodpecker-agent";
        };
      in
      {
        packages = {
          inherit woodpecker-server woodpecker-agent woodpecker-webui;
          default = woodpecker-server;
        };

        devShells.default =
          with pkgs;
          let
            go = go_1_26;
          in
          pkgs.mkShell {
            buildInputs = [
              # generic
              gnumake
              gnutar
              gzip
              zip
              tree

              # frontend
              nodejs_24
              pnpm
              typescript
              typescript-language-server

              # backend
              go
              glibc.static
              gofumpt
              golangci-lint
              go-mockery
              protobuf
              sqlite
              go-swag # for generate-openapi
              addlicense
              protoc-gen-go
              protoc-gen-go-grpc
              gcc

              # docs
              graphviz
            ];
            CFLAGS = "-I${pkgs.glibc.dev}/include";
            LDFLAGS = "-L${pkgs.glibc}/lib";
            GO = "${go}/bin/go";
            GOROOT = "${go}/share/go";
          };
      }
    );
}

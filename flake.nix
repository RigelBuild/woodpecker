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
        version = "3.17.0-rigel.25";

        # The repo requires Go 1.27 (go.mod toolchain); pin it so the sandboxed
        # build never tries to download a toolchain.
        buildGoModule = pkgs.buildGoModule.override { go = pkgs.go_1_27; };

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
        vendorHash = "sha256-6IDM+hYSXpD8hJlRgq3EhC/z6BaO7f+RUq8rFr1ud+I=";
        webuiHash = "sha256-bzvBMLnwuPKetCxRE7MY+pm1LB83N+mNY4ngSXhE4S0=";

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

        # Release binaries for this fork's tag, fetched instead of built. Each
        # sha256 is the matching line in the release checksums.txt:
        #   gh release download v3.17.0-rigel.25 -R RigelBuild/woodpecker -p checksums.txt
        # Bump these together with version.
        releaseAssets = {
          aarch64-darwin = {
            platform = "darwin_arm64";
            agentSha256 = "52138c6f3ec8437a8574964f6fa27db60aaaee619f89d78f9d102d10670c4fa9";
            cliSha256 = "99e8dc9f188e0447cd5f0d5d2d5e609cc2fdc37edf92143b34b6d363b90508ed";
          };
          x86_64-linux = {
            platform = "linux_amd64";
            agentSha256 = "7b4522916db85b7bb5c4a4c92e128a8498a23fc813f1b96ecd8e3d893d9cdb63";
            cliSha256 = "36a402920f568af2940b4a90dfc3c15f93a16f5b915c7c70df59359628d2aba0";
          };
          aarch64-linux = {
            platform = "linux_arm64";
            agentSha256 = "7b236d4c42ca0564031f6dc26926b2964b69a746d169ab7d0cfdd7a8772c4fb2";
            cliSha256 = "96a6eb361eae7cb9515d915c70b39eaafcd4aed6802a7379c12994c4e4e644f7";
          };
        };

        # Installs one fetched release binary as bin/woodpecker-<cmd>, the same
        # name the source builds get from postInstall. Nothing is compiled.
        fetchReleaseBin =
          cmd: platform: sha256:
          pkgs.stdenv.mkDerivation {
            pname = "woodpecker-${cmd}-bin";
            inherit version;

            src = pkgs.fetchurl {
              url = "https://github.com/RigelBuild/woodpecker/releases/download/v${version}/woodpecker-${cmd}_${platform}.tar.gz";
              inherit sha256;
            };

            dontUnpack = true;
            dontBuild = true;

            installPhase = ''
              runHook preInstall
              tar -xzf "$src"
              install -Dm755 "woodpecker-${cmd}" "$out/bin/woodpecker-${cmd}"
              runHook postInstall
            '';

            meta.mainProgram = "woodpecker-${cmd}";
          };

        releaseBins = pkgs.lib.optionalAttrs (builtins.hasAttr system releaseAssets) (
          let
            asset = releaseAssets.${system};
          in
          {
            woodpecker-agent-bin = fetchReleaseBin "agent" asset.platform asset.agentSha256;
            woodpecker-cli-bin = fetchReleaseBin "cli" asset.platform asset.cliSha256;
          }
        );
      in
      {
        packages = {
          inherit woodpecker-server woodpecker-agent woodpecker-webui;
          default = woodpecker-server;
        } // releaseBins;

        # Linux only: the check binds loopback ports, which the darwin sandbox restricts.
        checks = pkgs.lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
          woodpecker-server-boot = import ./nix/server-boot-check.nix {
            inherit pkgs woodpecker-server;
          };
        };

        devShells.default =
          with pkgs;
          let
            go = go_1_27;

            # rebuild Go based tools with the go version above
            buildGoModule = pkgs.buildGoModule.override { inherit go; };
            withGo =
              pkg:
              let
                builderArgs = lib.filterAttrs (name: _: lib.hasPrefix "buildGo" name) (
                  if pkg ? override then lib.functionArgs pkg.override else { }
                );
              in
              if builderArgs == { } then pkg else pkg.override (lib.mapAttrs (_: _: buildGoModule) builderArgs);
          in
          pkgs.mkShell {
            buildInputs = map withGo [
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
            STATIC_BUILD = "false";
            pnpm_config_pm_on_fail = "ignore";
          };
      }
    );
}

# AGENTS.md

The **deplo server agent**: one static Go binary per server that owns the host-coupled half of the
platform (Docker, the build pipeline, host metrics, backups) and exposes it to the control plane
over mTLS gRPC. Module `github.com/DeploCloud/deplo-agent`, AGPL-3.0-only.

**The architecture boundary is documented in the control-plane repo, not here.** Read
`../deplo/AGENTS.md` for the rules and `../deplo/CONTEXT.md` for the vocabulary (App, Project,
Capability, server agent - use those words), and
`../deplo/docs/adr/0006-server-agent-is-a-per-host-go-binary.md` for why this binary exists. The
control plane never touches a Docker socket; everything host-coupled arrives here as an RPC.

## Layout

- `main.go` - flags, mTLS config or first-run call-home bootstrap, gRPC wiring.
- `internal/server/` - the Agent service implementation. The bulk of the code.
- `internal/dockercli/`, `internal/hostmetrics/`, `internal/hostinfo/`, `internal/s3client/`,
  `internal/safepath/`, `internal/bootstrap/` - leaf helpers.
- `proto/agent.proto` - the wire contract, owned here.
- `gen/` - generated. Never hand-edit; `make proto` owns it.

## Reporting your work

**Say what you are about to do, then recap.** Before you start, one line on what you are about
to do; brief updates while you work help the user follow along. Close with a short recap that
stands on its own - what you found, what you did, and what is next - so a reader who only sees the
last message has the full picture.

## Comments

Few and short. **Hard cap about 3 lines per block.** No file-header essays, no design narratives, no
benchmark write-ups, no competitor comparisons. Where a feature has a docs page, one link replaces
the explanation:

```go
// https://deplo.build/docs/guides/backups-and-restore
```

Go doc comments on exported identifiers stay and keep their `// Identifier ...` opening - capped at
3 lines like everything else. Pragmas (`//go:build`, `//nolint`) and `ponytail:` markers are code,
not prose: never delete them.

**Never name a competitor.** Not in a comment, not in a string. The names that stay are functional:
a build tool (`nixpacks`, `railpack`), an image ref (`heroku/builder:24`), and the source paths of
the migration importer, which have to match what is on the source host.

## Commits

[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/):
`type(scope): imperative lowercase summary`. **Title 50 characters or fewer, no trailing period.**
Body only when the why does not fit the title, 2-3 lines at most. Commit straight to `main`; never
create a branch.

```
fix(volumecopy): refuse an export of a missing volume
feat(logs): stamp each log line with its timestamp
chore(proto): regenerate the stubs
```

## The proto regen dance

`proto/agent.proto` has two consumers and both must move together, or the wire contract silently
splits.

```sh
make proto                 # or: bash proto/generate.sh, where make is absent
```

It writes `gen/*.pb.go` HERE and `../deplo/lib/agent/gen/agent.ts` THERE.

Needs `protoc`, `protoc-gen-go` and `protoc-gen-go-grpc` on PATH, plus a sibling `../deplo` checkout
that has run `bun install` (ts-proto comes from its `node_modules`; override with
`DEPLO_REPO=/path/to/deplo`). If the TS plugin is missing, the script prints `SKIPPED TS stubs` and
**still exits 0** - check for that line. **Use the plugin versions already installed**; reinstalling
one restamps every generated file. Commit `gen/` here and `lib/agent/gen/agent.ts` there, together.

**Proto comments are not private.** protoc copies every leading comment into `gen/agent.pb.go` and
into the TypeScript client, so the 3-line cap applies to `agent.proto` too. Free-floating comments
are dropped by both generators; the file header reaches the Go stubs only.

## Build and verify

```sh
make build       # -> bin/deplo-agent (static, CGO_ENABLED=0)
make vet         # go vet ./...
make fmt-check   # gofmt + Prettier, what CI runs
```

**`go test ./...` drives real Docker on this host** - it pulls images, creates volumes and
containers, and the `TestE2E_*` tests stand up real databases. Do not run it casually on a machine
that also serves production. For a change that cannot alter behaviour, the verification is:

```sh
gofmt -l $(git ls-files '*.go')
go vet ./...
for a in amd64 arm64; do CGO_ENABLED=0 GOOS=linux GOARCH=$a go build -o /dev/null .; done
```

A pre-commit hook (`.githooks/pre-commit` -> lint-staged) runs `gofmt -w` on staged Go and Prettier
on staged Markdown and YAML. CI additionally runs `govulncheck` and the full test suite.

## Releases

The git tag is the only version source - `git describe` stamps the binary, there is no version file.
A `v*` tag publishes `deplo-agent-linux-amd64`, `deplo-agent-linux-arm64` and `checksums.txt`; the
control plane resolves the latest release and pins the checksum, so those asset names are a
contract. **Never tag on your own initiative.**

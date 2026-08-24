# The Deplo server agent (PLAN Part A / ADR-0006). A single static Go binary,
# one per server, that owns the host-coupled half of the platform over mTLS gRPC.
#
#   make build     -> ./bin/deplo-agent (static, this platform)
#   make proto     -> regenerate Go + TS stubs from proto/agent.proto
#   make test      -> go test ./...
#   make vet       -> go vet ./...
#   make fmt       -> gofmt the Go, Prettier the docs and workflows
#   make fmt-check -> the same, as a verdict (what CI runs)

# Stamp the agent version from the git tag — the SINGLE source of truth. A clean
# release checkout (`make build` at tag v1.2.0) stamps "1.2.0"; a dev checkout
# stamps the nearest tag + commits-ahead + short SHA + `-dirty` (e.g.
# "1.2.0-3-gabc1234-dirty"), or "dev" with no tags/git. The leading v is stripped
# so it matches the release assets + how the control plane normalizes tags
# (lib/agent/release.ts in IdraDev/deplo, which resolves "latest" from this repo's
# GitHub releases). The release workflow stamps from the tag directly, not this.
# Override with `make build VERSION=x`.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null | sed 's/^v//' || echo dev)
LDFLAGS := -X github.com/DeploCloud/deplo-agent/internal/server.AgentVersion=$(VERSION)

.PHONY: build test vet fmt fmt-check proto clean

build:
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/deplo-agent .

test:
	go test ./...

vet:
	go vet ./...

# Regenerate BOTH sides of the contract: the Go agent stubs (here) AND the TS
# control-plane client (written to ../deplo/lib/agent/gen when that checkout is a
# sibling — see proto/generate.sh). Commit the Go stubs here; copy the TS stub
# into the control-plane repo.
proto:
	bash proto/generate.sh

# Two formatters because there are two languages here: gofmt owns the 105 Go
# files, Prettier owns the Markdown and the workflows. The pre-commit hook runs
# both on whatever is staged; these targets are for doing the whole tree at once.
fmt:
	gofmt -w $(shell git ls-files '*.go')
	bunx prettier --write .

fmt-check:
	@out=$$(gofmt -l $(shell git ls-files '*.go')); \
	if [ -n "$$out" ]; then echo "gofmt wants these files:"; echo "$$out"; exit 1; fi
	bunx prettier --check .

clean:
	rm -rf bin

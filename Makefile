# The Deplo server agent (PLAN Part A / ADR-0006). A single static Go binary,
# one per server, that owns the host-coupled half of the platform over mTLS gRPC.
#
#   make build     -> ./bin/deplo-agent (static, this platform)
#   make proto     -> regenerate Go + TS stubs from proto/agent.proto
#   make test      -> go test ./...
#   make vet       -> go vet ./...

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

.PHONY: build test vet proto clean

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

clean:
	rm -rf bin

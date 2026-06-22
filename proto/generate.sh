#!/usr/bin/env bash
# Regenerate the agent gRPC stubs for BOTH sides from proto/agent.proto.
#
# The contract lives HERE (the agent owns its wire format). It has two consumers:
#   1. the Go agent in THIS repo            -> gen/ (committed here)
#   2. the TS control-plane client in the   -> <deplo>/lib/agent/gen/agent.ts
#      IdraDev/deplo repo                       (committed THERE)
#
# Because the two repos are separate, this script writes the Go stubs in place and
# writes the TS stub into a sibling checkout of the control-plane repo, which you
# then commit over there. Point DEPLO_REPO at that checkout if it is not ../deplo.
# Run this after editing agent.proto; commit gen/ here AND the TS stub in deplo.
#
# Requires on PATH: protoc, protoc-gen-go, protoc-gen-go-grpc (Go side, from
# `go install`). The TS side uses ts-proto from the control-plane repo's
# node_modules, so that repo must have had `bun install` / `npm install` run.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # this repo's root
PROTO_DIR="$ROOT/proto"
GO_OUT="$ROOT/gen"

# The control-plane checkout that owns the TS client. Override with DEPLO_REPO.
DEPLO_REPO="${DEPLO_REPO:-$ROOT/../deplo}"
TS_OUT="$DEPLO_REPO/lib/agent/gen"
TS_PROTO_BIN="$DEPLO_REPO/node_modules/.bin/protoc-gen-ts_proto"

# Make the Go toolchain discoverable whether or not the caller exported it.
export PATH="$PATH:/usr/local/go/bin:${GOPATH:-$HOME/go}/bin"

mkdir -p "$GO_OUT"

echo "[generate] Go stubs -> gen/"
protoc \
  --proto_path="$PROTO_DIR" \
  --go_out="$GO_OUT" --go_opt=paths=source_relative \
  --go-grpc_out="$GO_OUT" --go-grpc_opt=paths=source_relative \
  agent.proto

if [ -x "$TS_PROTO_BIN" ]; then
  echo "[generate] TS stubs -> $TS_OUT"
  mkdir -p "$TS_OUT"
  # ts-proto with grpc-js client/server stubs. The options match the control
  # plane's hand-written client wrapper; keep them in sync if you change them.
  protoc \
    --proto_path="$PROTO_DIR" \
    --plugin=protoc-gen-ts_proto="$TS_PROTO_BIN" \
    --ts_proto_out="$TS_OUT" \
    --ts_proto_opt=outputServices=grpc-js,esModuleInterop=true,useOptionals=messages,snakeToCamel=true \
    agent.proto
  echo "[generate] commit $TS_OUT/agent.ts in the control-plane repo (IdraDev/deplo)."
else
  echo "[generate] SKIPPED TS stubs: $TS_PROTO_BIN not found." >&2
  echo "[generate] Set DEPLO_REPO to a control-plane checkout with deps installed," >&2
  echo "[generate] then re-run to refresh lib/agent/gen/agent.ts there." >&2
fi

echo "[generate] done"

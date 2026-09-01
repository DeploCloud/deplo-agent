## What this changes

<!-- One or two sentences. If it fixes an issue, write "Fixes #123". -->

## How you tested it

<!-- What you actually ran, and on what. "The tests pass" on its own is not a test of the change. -->

## Checklist

- [ ] `make fmt-check` passes
- [ ] `go vet ./...` passes
- [ ] `go test ./...` passes (on a host with a Docker daemon you can afford to disturb)
- [ ] `govulncheck ./...` is clean
- [ ] It still builds static for `linux/amd64` and `linux/arm64`
- [ ] I added tests for the new behaviour, or said in the pull request why it cannot be tested
- [ ] A new RPC is additive and gated behind a `Hello` capability, so an older control plane still works
- [ ] This does not move port routing, compose rendering or Traefik labels into the agent (ADR-0006)
- [ ] I read [CONTRIBUTING.md](../CONTRIBUTING.md) and signed the [CLA](https://github.com/DeploCloud/deplo/blob/main/CLA.md) (the bot asks below on your first pull request)

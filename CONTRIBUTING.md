# Contributing to deplo-agent

This repository is the **server agent**: one static Go binary per host, the only thing in
Deplo that runs `docker`, a shell or `fs` on a server. The control plane lives in
[DeploCloud/deplo](https://github.com/DeploCloud/deplo).

## Where things go

- **Questions and ideas** go to
  [Discussions](https://github.com/DeploCloud/deplo/discussions) on the control-plane repo,
  where everyone already is.
- **Bugs and feature requests about the agent** go to
  [Issues](https://github.com/DeploCloud/deplo-agent/issues) here.
- **Security vulnerabilities** go to [SECURITY.md](SECURITY.md), never to a public issue.
  The gRPC port is reachable by anyone who can open a socket, so treat a finding here as
  pre-auth until proven otherwise.

## Running it locally

You need Go (the version in [`go.mod`](go.mod), which is also the toolchain pin) and, for
the docs and workflow formatting, [Bun](https://bun.sh).

```bash
make build   # -> ./bin/deplo-agent, static, this platform
make test    # go test ./...
make vet     # go vet ./...
make fmt     # gofmt the Go, Prettier the Markdown and the workflows
make proto   # regenerate the Go + TS stubs from proto/agent.proto
```

`go test ./...` talks to a real Docker daemon. Run it on a machine you can afford to
disturb, never on a host running Deplo workloads.

## Before you open a pull request

CI runs exactly these, and a green run is what gates a release tag:

```bash
make fmt-check
go vet ./...
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
go test ./...
```

Plus a static build for both target arches (`linux/amd64`, `linux/arm64`).

**New behaviour ships with its tests, in the same commit.** If something genuinely cannot
be tested, say so in the commit body.

## Read these first

- **[AGENTS.md](AGENTS.md)** - how this repository is laid out and what belongs in it.
- **ADR-0006** (in the control-plane repo) - the boundary. The agent owns everything
  host-coupled; it must **never** grow port routing, compose rendering or Traefik label
  logic, which stay control-plane side and arrive here as opaque YAML.
- **New RPCs are additive.** The contract stays `V1`, and a host feature is gated behind a
  `Hello` capability so an older agent is named rather than failing mid-deploy.
- **Releases only move forward.** The fleet never downgrades.

## Commits

[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) with a scope:
`type(scope): imperative lowercase summary`, title 50 characters or fewer, no trailing
period. Body only when the _why_ does not fit the title.

## Licensing your contribution

deplo-agent is **AGPL-3.0-only**, and DeploCloud also offers Deplo under commercial terms.
For that to keep working, every contribution has to arrive with the right to do both.

That right is the **[CLA](https://github.com/DeploCloud/deplo/blob/main/CLA.md)**, and a
bot asks for it on your first pull request here: you read the document and post one
sentence as a comment.

```
I have read the CLA Document and I hereby sign the CLA
```

Once per person in this repository, covering everything you open here afterwards and
everything you contributed here before. **You keep the copyright to what you wrote** - it
is a license grant, not an assignment. If your employer owns your work, get their sign-off
before signing.

The **name "deplo" and the logo are not part of the license**: fork the code freely, rename
your fork.

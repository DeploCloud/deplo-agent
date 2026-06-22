# Migration: splitting the agent out of IdraDev/deplo

The agent (Go binary + its proto contract) moved out of the `IdraDev/deplo`
monorepo into this repo. The control plane now consumes the agent as **GitHub
Release assets** instead of building it. This file is the one-time cutover
checklist. Delete it once the cutover is done.

## 1. Publish this repo

This tree is ready: module path `github.com/DeploCloud/deplo-agent`, tests
green, release + CI workflows in `.github/workflows/`.

```bash
cd deplo-agent
git init -b main
git add -A
git commit -m "Import agent + proto from IdraDev/deplo; release as GitHub assets"
git remote add origin https://github.com/DeploCloud/deplo-agent.git
git push -u origin main
```

## 2. Cut the first release

The control plane resolves the **latest** release, so nothing installs until one
exists. The git tag is the only version source — it is stamped into the binary
directly, so just pick the tag.

```bash
git tag v1.1.0
git push origin v1.1.0
```

The `release` workflow builds `deplo-agent-linux-amd64`, `deplo-agent-linux-arm64`
and `checksums.txt` and attaches them to the release. Confirm all three assets
appear on the release page — the control plane depends on those exact names.

## 3. Clean up the control plane (IdraDev/deplo)

The control-plane changes are already committed there (release-based install,
Dockerfile downloads the binary, `lib/agent/release.ts`, etc.). The only thing
left is to delete the now-duplicated source. Verify nothing references it first:

```bash
cd ../deplo
git rm -r agent/ proto/
# The control plane keeps lib/agent/ (its TS client) and the COMMITTED
# generated stub lib/agent/gen/agent.ts — do NOT delete those.
grep -rn '"\.\./agent\|/agent/version.json\|COPY agent' --include='*.ts' --include='*.tsx' Dockerfile . \
  | grep -v node_modules | grep -v lib/agent      # expect: no matches
bun run test && bunx tsc --noEmit                  # expect: green
```

`agent/version.json` is gone; `lib/version.ts` already stopped importing it (it
uses `FALLBACK_AGENT_VERSION` + the live release lookup).

## 4. Wire up regeneration for future proto changes

The contract now lives here (`proto/agent.proto`). When you change it:

```bash
# in this repo, with a sibling ../deplo checkout that has had `bun install`:
make proto            # regenerates gen/ here AND ../deplo/lib/agent/gen/agent.ts
git add gen/ && git commit -m "proto: <change>"      # commit Go stubs here
cd ../deplo && git add lib/agent/gen/agent.ts && git commit -m "proto: regen TS client"
```

Set `DEPLO_REPO=/path/to/deplo` if the control-plane checkout isn't `../deplo`.
Both sides must be regenerated together — a proto change that lands here but not
in the control plane (or vice-versa) silently breaks the wire contract.

## 5. Operational notes

- **Always-latest policy.** Publishing a release immediately changes what new
  servers install (`lib/agent/release.ts` resolves `releases/latest`). There is
  no control-plane gate beyond the checksum pin (integrity, not correctness) — so
  test a release before tagging. To switch to an explicit pin, change the
  `releases/latest` lookup in `lib/agent/release.ts` to `releases/tags/<tag>`.
- **The control-plane image bakes latest-at-build.** Its own local agent is
  whatever was latest when the image was built; new remote installs get true
  latest. Drift between them is surfaced by the dashboard's agent badge, not
  hidden. Rebuild the control-plane image to refresh its local agent.
- **`FALLBACK_AGENT_VERSION`** in `../deplo/lib/agent/release.ts` is only used
  when GitHub is unreachable; keep it roughly in step with real releases.

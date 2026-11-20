# 05 — Build and release

How the fake-tickets image is built, tested, published and deployed. The same Dockerfile serves two paths:

- **By hand with podman**, to understand or demonstrate each step.
- **GitHub Actions**, the normal release path: a version tag publishes the image, then one commit deploys it.

```
 git tag v2.0.1 ─► GitHub Actions ─► ghcr.io/romainanselin/fake-tickets:2.0.1 ─┐
                   (test stage, build,                                         │
                    SBOM, provenance)                                          ▼
 commit newTag: 2.0.1 in deploy/k8s/overlays/holonet ─► ArgoCD (k1) ─► rolling update of backend + frontend
```

---

## 1. The image

| | |
|---|---|
| Name | `ghcr.io/romainanselin/fake-tickets` (public) |
| Tags | `2.0.1` (release), `2.0` (latest patch of 2.0), `sha-<short commit>`, `latest` (newest release; never used by the deployment, which pins a version) |
| Base | UBI 9: `ubi9/python-312` to build the virtualenv, `ubi9/ubi-minimal` + the `python3.12` RPM to run it. Both pinned by tag in the `Dockerfile` |
| Roles | One image, two processes: `faketickets backend` (port 8444), `faketickets frontend` (port 8443) |
| Runtime | uid 10001, gid 0, read-only root filesystem, data in `/data`, `dumb-init` as PID 1 |
| CPU | x86-64-v2 is enough (UBI 9). UBI 10 images need x86-64-v3 |

The `Dockerfile` has three stages:

| Stage | What it does | Used by |
|---|---|---|
| `build` | Creates `/opt/venv`, installs the dependencies first (cached layer), then the package; imports the apps to fail early | both |
| `test` | Copies `tests/`, removes the source tree and runs `pytest` against the **installed** package, so a packaging mistake fails the build | `--target test` only |
| `runtime` | ubi-minimal + Python 3.12 + the venv from `build`, non-root user, labels | the published image |

## 2. Build by hand (podman)

From the repo root on any Linux machine with podman:

```bash
# 1. Tests, inside the image build. A failing test fails the build.
podman build --target test -t localhost/fake-tickets:test .

# 2. The runtime image. VERSION ends up in the org.opencontainers.image.version label.
podman build --build-arg VERSION=2.0.0 -t localhost/fake-tickets:2.0.0 .

# 3. Look at it
podman image inspect localhost/fake-tickets:2.0.0 --format '{{json .Labels}}' | jq
podman run --rm localhost/fake-tickets:2.0.0 workloads | head     # the workload catalogue
podman run --rm localhost/fake-tickets:2.0.0 genkey               # a random key, e.g. for FT_BACKEND_TOKEN

# 4. Run the whole stack locally: see README.md "Containers" (podman compose)
```

### Push by hand (only if CI is unavailable)

Normal releases go through CI (§3). To push from a workstation you need a GitHub **classic** token with
`write:packages`, typed at the prompt and never stored:

```bash
podman login ghcr.io -u <github-user>        # paste the token at the prompt
podman tag localhost/fake-tickets:2.0.0 ghcr.io/romainanselin/fake-tickets:2.0.0
podman push ghcr.io/romainanselin/fake-tickets:2.0.0
podman logout ghcr.io
```

A hand-pushed image has no SBOM or provenance, and nothing ties it to a commit. Prefer the tag.

## 3. Build in GitHub Actions

Workflow: `.github/workflows/image.yml`.

| Trigger | Test stage | Runtime build | Push |
|---|---|---|---|
| Pull request | yes | yes | no |
| Push to `main` | yes | yes | no |
| Tag `v*` (e.g. `v2.0.1`) | yes | yes | **yes**: `2.0.1`, `2.0`, `sha-<short>`, `latest` |
| Manual (Actions tab → *image* → Run workflow) | yes | yes | only if run on a tag |

What it does, in order:

1. Reads the version from `pyproject.toml`. On a tag, `v<version>` must equal the tag, or the job fails: the
   image label, the Python package and the image tag always agree.
2. Builds the `test` stage (the unit suite runs inside it).
3. Computes the tags and OCI labels (`docker/metadata-action`).
4. On a tag only: logs in to ghcr.io with the workflow's own `GITHUB_TOKEN`. Its `packages: write` permission lasts
   for this run only. **No personal token is stored in the repository.**
5. Builds the `runtime` stage for `linux/amd64` and, on a tag, pushes it with:
   - an **SBOM** (SPDX list of every package in the image),
   - **build provenance** (SLSA: which workflow, commit and inputs produced it).

Layer cache: GitHub Actions cache (`type=gha`), so an unchanged dependency layer isn't rebuilt.

Check what was published:

```bash
# tags
gh api /users/RomainAnselin/packages/container/fake-tickets/versions --jq '.[].metadata.container.tags'
# SBOM and provenance attached to the image
docker buildx imagetools inspect ghcr.io/romainanselin/fake-tickets:2.0.1 --format '{{json .SBOM}}' | jq '.SPDX.packages | length'
docker buildx imagetools inspect ghcr.io/romainanselin/fake-tickets:2.0.1 --format '{{json .Provenance}}' | jq '.SLSA.invocation.configSource'
```

### Package visibility

The first push (v2.0.0, 2026-11-17) created the package **public** and linked to this repository: it inherited the
repository's visibility, through the image's `org.opencontainers.image.source` label. Anyone can pull it, no pull
secret. If a package ever shows as private: GitHub → Packages → `fake-tickets` → Package settings → Change
visibility. Check anonymously:

```bash
skopeo inspect --no-creds docker://ghcr.io/romainanselin/fake-tickets:2.0.0 | jq -r '.Labels["org.opencontainers.image.version"]'
```

## 4. Release

```bash
# 1. Bump the version
#    pyproject.toml: version = "2.0.1"      (the Dockerfile's VERSION default is only for hand builds)
#    deploy/k8s/overlays/lab/kustomization.yaml: images[0].newTag: 2.0.1
git commit -am "Release 2.0.1" && git push

# 2. Tag: this publishes the image
git tag v2.0.1 && git push origin v2.0.1
#    wait for the "image" workflow to go green (Actions tab, or: gh run watch)

# 3. Deploy: point the lab overlay at the new tag
#    deploy/k8s/overlays/holonet/kustomization.yaml: images[0].newTag: 2.0.1
git commit -am "Deploy 2.0.1 to the lab" && git push
#    ArgoCD (k1) sees the commit within its poll interval (or press Refresh on fake-tickets) and rolls out
#
#    The generic overlay deploy/k8s/overlays/lab is bumped in the release commit (step 1), so the tag carries
#    it: worklab sites pin the tag in their ArgoCD Application (targetRevision) and move by changing it.
```

Deployment is a Git change, never `kubectl set image`: ArgoCD's self-heal would revert a hand change. To roll back,
revert the `newTag` commit.

Both Deployments use `strategy: Recreate` (SQLite and the run processes need a single pod), so a release means a
short outage of the UI and **interrupts running workloads**: they show as `interrupted` and the next run on the same
table offers to resume. Release when no long run is going.

Config-only changes (the `configMapGenerator` literals in the overlay) update the ConfigMap but don't restart the
pods: the base ConfigMap is a plain resource, so its name has no content hash. Restart after such a change:
`kubectl -n fake-tickets rollout restart deploy/backend deploy/frontend`.

## 5. Maintenance

| Task | How |
|---|---|
| Update the base images | `skopeo list-tags docker://registry.access.redhat.com/ubi9/ubi-minimal` (and `ubi9/python-312`), pick the newest `9.x-<release>`, change the two `ARG` lines in the `Dockerfile`, open a PR (CI builds and tests it), then release |
| Update Python dependencies | `pyproject.toml`, PR, release |
| Update an Action | Change the `@vN` in `image.yml` after reading its release notes |
| Remove a bad release | Delete the version in the package settings, then make sure no overlay points at it |
| The old v1 image | `ghcr.io/romainanselin/fake_tickets_app:latest` (2025-12, private) predates v2 and isn't used. Delete it when convenient |

## 6. Where it runs

The lab deployment (overlay `deploy/k8s/overlays/holonet`, ArgoCD Application on k1) is described in the homelab
repository: `docs/fake-tickets.md`. Other labs with the same stack use the generic overlay `deploy/k8s/overlays/lab`:
every site value is a placeholder that the ArgoCD Application patches (worklab's `apps/fake-tickets` renders it),
and the Application pins a release tag rather than `main`. The base (`deploy/k8s/base`) is generic and can be used on any cluster with its
own overlay.

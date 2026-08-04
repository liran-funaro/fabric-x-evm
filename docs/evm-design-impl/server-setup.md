# Server Setup (native experiment host)

Authoritative performance numbers are taken on **native hardware** — QEMU
emulation (colima on the Mac) adds a ~3.7× latency tax and its own wedges (see
[findings.md](findings.md) §5). This doc provisions a fresh SSH host and syncs
the repo to it.

## Workflow model (binding)

- **The Mac is the source of truth.** Edit code locally; `go build` /
  `go test` / `-race` locally. Run *experiments* on the remote over SSH.
- **Never edit code on the remote.** Always sync **local → remote**; local/git
  always wins. The remote working tree is disposable.
- **Data lives outside the repo** at `$EVM_PERF_DATA`
  (`~/workspace/evm-perf-data`), fetched by `scripts/setup.sh` on the remote —
  so the rsync never carries the multi-hundred-MB datasets.

## Step 0 — Ask which server first

There is no hardcoded host. **Ask the user which server to use before touching
anything** (historically `ec2` — RHEL 10, native x86_64, 32 vCPU / 61 GB,
reachable as `ssh ec2` — but it can be any SSH host). Confirm the SSH alias and
that provisioning it is expected before proceeding.

## Step 1 — Sync the repo (local → remote)

From the Mac, at the repo root. Exclude data, git, and gitignored scratch:

```bash
SERVER=ec2          # <-- the confirmed host from Step 0
DEST="\$HOME/workspace/fabric-x-evm"

ssh "$SERVER" "mkdir -p $DEST"
rsync -az --delete \
  --exclude '.git/' \
  --exclude 'data/' \
  --exclude '.superpowers/' \
  --exclude '*.out' --exclude '*.log' --exclude '*.prof' \
  ./ "$SERVER:$DEST/"
```

`--delete` keeps the remote tree byte-identical to local (local wins). Re-run
this after every local edit before re-running an experiment.

## Step 2 — Provision the host (idempotent)

Only needed once per fresh host. Runs on the remote:

```bash
ssh "$SERVER"
cd ~/workspace/fabric-x-evm
bash scripts/setup-server.sh
```

`scripts/setup-server.sh` (safe to re-run):
- Installs **Docker** (dnf on RHEL-family, apt on Debian/Ubuntu) and enables
  the daemon.
- Installs the repo's **Go toolchain** (version read from `go.mod`, currently
  `1.26.5`) into `/usr/local/go` and puts it on PATH.
- Adds the current user to the **docker** group.
- On RHEL-family kernels, ensures the **netfilter modules** dockerd's bridge
  network needs (`br_netfilter` / `iptable_nat` / `xt_addrtype`, shipped in
  `kernel-modules-extra`) are loadable.

### The RHEL 10 reboot gotcha

Installing `docker-ce` on RHEL 10 can pull a **newer kernel** whose
`kernel-modules-extra` (carrying the netfilter modules) only apply to that
not-yet-booted kernel. `dockerd`'s bridge network will not start until you
**reboot into the updated kernel**. The script detects an unloadable module
and prints an ACTION-REQUIRED notice. If you see it:

```bash
sudo reboot          # then re-connect
```

Re-login (or `newgrp docker`) is also required so docker-group membership takes
effect without sudo. After the reboot/re-login, `source ~/.bashrc` (or start a
fresh shell) so `go` is on PATH.

## Step 3 — Fetch datasets on the remote

```bash
export EVM_PERF_DATA="$HOME/workspace/evm-perf-data"
bash scripts/setup.sh          # downloads contract + both datasets; skips present files
```

## Step 4 — Run experiments

Follow [experiments.md](experiments.md): `make clean-x init-x start-full`,
confirm Prometheus (:9090) + Grafana (:3000, EVM dashboard), then run the
replay for **both** datasets and collect EVM tx/s + committer tx/s + phase /
commit / abort metrics.

## Step 5 — Commit only from local

When a run is green and you want to keep a code change, commit it **on the Mac**
(never on the remote), then re-sync. Branch `bft-redesign` is **not pushed**.

---

### Reference: known-good `ec2` toolchain

Recorded from the working host, for comparison when debugging a new box:
Go 1.26.5, Docker 29.6.2 (cgroup v2, overlayfs), docker compose v5.3.1, RHEL
10.2. Stack bring-up (`clean-x init-x start-full`, container-local) ≈ 57 s, 24
containers healthy.

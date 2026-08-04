#!/usr/bin/env bash
#
# Idempotent provisioner for a FRESH experiment host (native hardware).
#
# Runs ON the remote host (not on the Mac). It installs Docker + the repo's Go
# toolchain, adds the current user to the docker group, and — on RHEL-family
# kernels — ensures the netfilter modules dockerd's bridge network needs are
# present. It does NOT reboot or log you out; it PRINTS what still needs a
# restart/re-login and stops, because those are machine-wide actions the
# operator should confirm.
#
# Usage (after syncing the repo to the remote — see docs/evm-design-impl/server-setup.md):
#   ssh <server>
#   cd ~/workspace/fabric-x-evm
#   bash scripts/setup-server.sh
#
# Re-running is safe: every step checks for the tool/version first and skips it.
set -euo pipefail

# --- Go version: prefer the repo's go.mod, fall back to the pinned default ----
GO_VERSION_DEFAULT="1.26.5"
GO_VERSION="$GO_VERSION_DEFAULT"
if [ -f go.mod ]; then
	v="$(awk '/^go [0-9]/ {print $2; exit}' go.mod || true)"
	[ -n "$v" ] && GO_VERSION="$v"
fi

ARCH="$(uname -m)"
case "$ARCH" in
	x86_64)  GOARCH="amd64" ;;
	aarch64|arm64) GOARCH="arm64" ;;
	*) echo "unsupported arch: $ARCH" >&2; exit 1 ;;
esac

# --- Detect distro family ------------------------------------------------------
. /etc/os-release 2>/dev/null || { echo "cannot read /etc/os-release" >&2; exit 1; }
FAMILY="unknown"
case "${ID:-} ${ID_LIKE:-}" in
	*rhel*|*fedora*|*centos*) FAMILY="rhel" ;;
	*debian*|*ubuntu*)        FAMILY="debian" ;;
esac
echo ">> host: ${PRETTY_NAME:-$ID} ($ARCH), family=$FAMILY, target Go $GO_VERSION"

REBOOT_HINT=0

# --- 1. Docker -----------------------------------------------------------------
if command -v docker >/dev/null 2>&1; then
	echo ">> docker present: $(docker --version)"
else
	echo ">> installing docker..."
	case "$FAMILY" in
		rhel)
			sudo dnf -y install dnf-plugins-core || true
			sudo dnf config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo 2>/dev/null \
				|| sudo dnf config-manager --add-repo https://download.docker.com/linux/rhel/docker-ce.repo
			sudo dnf -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
			# RHEL 10: docker-ce may pull a newer kernel; its netfilter modules
			# live in kernel-modules-extra and dockerd's bridge net needs them.
			sudo dnf -y install "kernel-modules-extra-$(uname -r)" 2>/dev/null \
				|| sudo dnf -y install kernel-modules-extra || true
			;;
		debian)
			sudo apt-get update
			sudo apt-get -y install ca-certificates curl gnupg
			sudo install -m 0755 -d /etc/apt/keyrings
			curl -fsSL "https://download.docker.com/linux/${ID}/gpg" | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
			sudo chmod a+r /etc/apt/keyrings/docker.gpg
			echo "deb [arch=$GOARCH signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/${ID} $(. /etc/os-release && echo "${VERSION_CODENAME}") stable" \
				| sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
			sudo apt-get update
			sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
			;;
		*)
			echo "!! unknown distro family; install Docker manually, then re-run" >&2
			exit 1
			;;
	esac
fi

sudo systemctl enable --now docker 2>/dev/null || true

# --- 2. netfilter modules (bridge network) ------------------------------------
# dockerd's default bridge needs br_netfilter / iptable_nat / xt_addrtype. On a
# freshly-updated RHEL kernel these ship in kernel-modules-extra for the RUNNING
# kernel; if that package was just installed for a not-yet-booted kernel, a
# reboot into it is required before dockerd's bridge works.
for m in br_netfilter iptable_nat xt_addrtype; do
	if ! sudo modprobe "$m" 2>/dev/null; then
		echo "!! kernel module '$m' not loadable for the running kernel ($(uname -r))"
		REBOOT_HINT=1
	fi
done

# --- 3. docker group -----------------------------------------------------------
if id -nG "$USER" | tr ' ' '\n' | grep -qx docker; then
	echo ">> $USER already in docker group"
else
	echo ">> adding $USER to docker group (takes effect after re-login)"
	sudo groupadd -f docker
	sudo usermod -aG docker "$USER"
	REBOOT_HINT=1
fi

# --- 4. Go toolchain -----------------------------------------------------------
NEED_GO=1
if command -v go >/dev/null 2>&1; then
	cur="$(go version | awk '{print $3}' | sed 's/^go//')"
	if [ "$cur" = "$GO_VERSION" ]; then
		echo ">> go $cur present"
		NEED_GO=0
	else
		echo ">> go $cur present but repo wants $GO_VERSION; replacing"
	fi
fi
if [ "$NEED_GO" = "1" ]; then
	tgz="go${GO_VERSION}.linux-${GOARCH}.tar.gz"
	echo ">> installing go $GO_VERSION ($GOARCH)..."
	curl -fsSL "https://go.dev/dl/${tgz}" -o "/tmp/${tgz}"
	sudo rm -rf /usr/local/go
	sudo tar -C /usr/local -xzf "/tmp/${tgz}"
	rm -f "/tmp/${tgz}"
	# Make go available on PATH for future shells (idempotent).
	if ! grep -qs '/usr/local/go/bin' "$HOME/.bashrc" 2>/dev/null; then
		echo 'export PATH=$PATH:/usr/local/go/bin' >> "$HOME/.bashrc"
	fi
	export PATH="$PATH:/usr/local/go/bin"
	echo ">> $(go version)"
fi

# --- 5. Summary / next steps ---------------------------------------------------
echo
echo "=============================================================="
echo "Provisioning done."
if [ "$REBOOT_HINT" = "1" ]; then
	echo
	echo "ACTION REQUIRED before running the stack:"
	echo "  - Re-login (or 'newgrp docker') so docker-group membership applies, AND/OR"
	echo "  - If a kernel module failed above, REBOOT into the updated kernel"
	echo "    (docker's bridge network won't start otherwise):  sudo reboot"
fi
echo
echo "Next:"
echo "  1. Ensure PATH has go:   source ~/.bashrc   (or re-login)"
echo "  2. Fetch datasets:       export EVM_PERF_DATA=\$HOME/workspace/evm-perf-data && bash scripts/setup.sh"
echo "  3. Run experiments:      see docs/evm-design-impl/experiments.md"
echo "=============================================================="

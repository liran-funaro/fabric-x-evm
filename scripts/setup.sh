#!/bin/bash
#
# One-time setup: download the perf contract and BOTH replay workloads into the
# out-of-tree data dir (EVM_PERF_DATA, default ~/workspace/evm-perf-data). This
# dir is the single source for input data AND the target for experiment output
# (results land under $EVM_PERF_DATA/results), so a repo sync never touches it.
#
# The perf test switches between the two workloads with the -dataset flag by name:
#   -dataset synthetic  -> USDC_dataset.synthetic.json.gz  (conflict-free; raw throughput ceiling)
#   -dataset historic   -> USDC_dataset.historic.json.gz   (real Jan-2020 USDC; extremely high conflict rate)
# Always measure BOTH workloads.
#
# Already-present files are skipped, so re-running is cheap. Force a re-download
# by deleting the file (or the whole dir) first.
set -euo pipefail

DATA_DIR="${EVM_PERF_DATA:-$HOME/workspace/evm-perf-data}"
BASE_URL="https://ale.sopit.net/tmp"

mkdir -p "$DATA_DIR"

# fetch <dest-filename> <source-filename>
fetch() {
	local dest="$DATA_DIR/$1"
	local url="$BASE_URL/$2"
	if [ -s "$dest" ]; then
		echo "skip (present): $dest"
		return
	fi
	echo "downloading $url -> $dest"
	curl --fail --location --output "$dest" "$url"
}

# USDC contract (prime DB): the proxy + implementation bytecode/metadata.
fetch USDC_contract.json USDC_contract.json

# Conflict-free synthetic dataset (raw throughput ceiling):
fetch USDC_dataset.synthetic.json.gz USDC_dataset.synthetic.json.gz

# Real ERC-20 USDC historic dataset (Jan 2020) -- extremely high conflict rate,
# exercises the MVCC rollback / re-batch path:
fetch USDC_dataset.historic.json.gz USDC_dataset.012020.json.gz

echo
echo "Data dir ready: $DATA_DIR"
echo "Point the tests and scripts at it by exporting:"
echo "    export EVM_PERF_DATA=$DATA_DIR"

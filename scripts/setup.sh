#!/bin/bash

# One-time setup: download the perf contract and BOTH replay workloads.
# The perf test switches between them with the -dataset flag by name:
#   -dataset synthetic  -> USDC_dataset.synthetic.json.gz  (conflict-free; raw throughput ceiling)
#   -dataset historic   -> USDC_dataset.historic.json.gz   (real Jan-2020 USDC; extremely high conflict rate)
# Always measure BOTH workloads.
mkdir -p ./integration/perf/testdata/

curl --output ./integration/perf/testdata/USDC_contract.json https://ale.sopit.net/tmp/USDC_contract.json

# Conflict-free synthetic dataset (raw throughput ceiling):
curl --output ./integration/perf/testdata/USDC_dataset.synthetic.json.gz https://ale.sopit.net/tmp/USDC_dataset.synthetic.json.gz

# Real ERC-20 USDC historic dataset (Jan 2020) -- extremely high conflict rate,
# exercises the MVCC rollback / re-batch path:
curl --output ./integration/perf/testdata/USDC_dataset.historic.json.gz https://ale.sopit.net/tmp/USDC_dataset.012020.json.gz

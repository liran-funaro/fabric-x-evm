#!/bin/bash

# One-time setup
mkdir -p ./integration/perf/testdata/

curl --output ./integration/perf/testdata/USDC_contract.json https://ale.sopit.net/tmp/USDC_contract.json

# Choose the dataset
# Conflict-free dataset:
curl --output ./integration/perf/testdata/USDC_dataset.json.gz https://ale.sopit.net/tmp/USDC_dataset.synthetic.json.gz

# Alternatively, the real ERC-20 USDC historic dataset.
# It has a extreamly high conflict rate.
# curl --output ./integration/perf/testdata/USDC_dataset.json.gz https://ale.sopit.net/tmp/USDC_dataset.012020.json.gz

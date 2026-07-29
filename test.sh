#!/bin/bash

export FABRIC_LOGGING_SPEC="info:grpc=error"

# Data storage (default: container-local, off colima's sshfs mount, which caps
# throughput). Set HOST_DATA=1 to persist committer + orderer state under ./data
# on the host for inspection:  HOST_DATA=1 ./test.sh
export HOST_DATA="${HOST_DATA:-0}"

# Start system
make clean-x init-x start-full

echo

# Run test
# -cpuprofile=cpu.out -blockprofile=block.out

go test -timeout 4h -tags=perf -run '^TestReplayJSONDataset$' -v \
  -count=1 ./integration/perf/... -gateway-config fabx-full.yaml -enable-metrics

#go test -timeout 4h -tags=perf -run '^TestReplayJSONDataset$' -v \
#  -count=1 ./integration/perf/... -gateway-config fabx-full.yaml \
#  -outstanding 10000 -dataset integration/perf/testdata/USDC_dataset.json.gz -oldqueue -namespace synthetic

echo

# Stop the test
make stop-full clean-x

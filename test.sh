#!/bin/bash

export FABRIC_LOGGING_SPEC="info:grpc=error"

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

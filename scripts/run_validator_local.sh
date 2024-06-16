#!/bin/bash

# Get the number of proxies and servers from command line arguments or use default values.
NUM_VALIDATORS=${1:-1}
SHARDS_PER_VALIDATOR=${2:-4}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONF_DIR="${SCRIPT_DIR}/../configuration/local"

cd $SCRIPT_DIR/..

# Clean up data files
rm -rf *.json *.txt *.db

# Make sure to clean up child processes on exit.
trap 'kill $(jobs -p)' EXIT

set -x

# Create configuration files for NUM_VALIDATORS validators with SHARDS_PER_VALIDATOR shards each.
# * Private server states are stored in `server*.json`.
# * `committee.json` is the public description of the Linera committee.
VALIDATOR_FILES=()
for i in $(seq 1 $NUM_VALIDATORS); do
    VALIDATOR_FILES+=("$CONF_DIR/validator_$i.toml")
done
./linera-server generate --validators "${VALIDATOR_FILES[@]}" --committee committee.json --testing-prng-seed 1

STORAGE="service:tcp:$LINERA_STORAGE_SERVICE:linera"

# Start servers and create initial chains in DB
for I in $(seq 1 $NUM_VALIDATORS)
do
    ./linera-proxy server_"$I".json --storage $STORAGE --genesis genesis.json &

    for J in $(seq 0 $((SHARDS_PER_VALIDATOR - 1)))
    do
        ./linera-server initialize --storage $STORAGE --genesis genesis.json
    done
    for J in $(seq 0 $((SHARDS_PER_VALIDATOR - 1)))
    do
        ./linera-server run --storage $STORAGE --server server_"$I".json --shard "$J" --genesis genesis.json &
    done
done

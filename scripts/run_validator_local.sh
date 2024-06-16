#!/bin/bash

# Get the number of proxies and servers from command line arguments or use default values.
# Default number of validators is 4, default number of shards per validator is 4.
NUM_VALIDATORS=${1:-4}
SHARDS_PER_VALIDATOR=${2:-4}

STORAGE="service:tcp:$LINERA_STORAGE_SERVICE:linera"

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONF_DIR="${SCRIPT_DIR}/../configuration/local"

set -x

# Create configuration files for NUM_VALIDATORS validators with SHARDS_PER_VALIDATOR shards each.
# * Private server states are stored in `server*.json`.
# * `committee.json` is the public description of the Linera committee.
VALIDATOR_FILES=()
for i in $(seq 1 $NUM_VALIDATORS); do
    VALIDATOR_FILES+=("$CONF_DIR/validator_$i.toml")
done
./linera-server generate --validators "${VALIDATOR_FILES[@]}" --committee committee.json --testing-prng-seed 37

# Create configuration files for 10 user chains.
# * Private chain states are stored in one local wallet `wallet.json`.
# * `genesis.json` will contain the initial balances of chains as well as the initial committee.
./linera --wallet wallet.json --storage rocksdb:linera.db create-genesis-config 10 --genesis genesis.json --initial-funding 10 --committee committee.json --testing-prng-seed 2

# Initialize the second wallet.
./linera --wallet wallet_2.json --storage rocksdb:linera_2.db wallet init --genesis genesis.json --testing-prng-seed 3

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

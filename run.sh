#!/bin/bash

set -o errexit -o nounset

# Read common EVN vars
export $(cat ../.env | sed '/^#/d' | xargs)


export RPC_ADDRESS=tcp://127.0.0.1:26657
export GRPC_ADDRESS=127.0.0.1:9092
export GRPC_TLS=false


export DATA_COLLECTION_MODE="event"  # available values {'pull' | 'event'}


go mod tidy  && reset && go build -o app . && ./app

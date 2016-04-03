#!/bin/bash

EXEC_DIR=$(pwd)
cd $(dirname $0)

CLI_PORT=10101
TOMP2P_PORT=10301

BOOTSTRAP_ADDRESS=193.0.109.30
BOOTSTRAP_TOMP2P_PORT=10301
BOOTSTRAP_PORT=10201

export COMMON_ARGS="--CLASS_NAME=org.nebulostore.systest.textinterface.TextInterface --BOOTSTRAP_ADDRESS=$BOOTSTRAP_ADDRESS --BOOTSTRAP_TOMP2P_PORT=$BOOTSTRAP_TOMP2P_PORT --BOOTSTRAP_PORT=$BOOTSTRAP_PORT --BOOTSTRAP_MODE=server --CLI_PORT=$CLI_PORT --TOMP2P_PORT=$TOMP2P_PORT --BDB_TYPE=storage-holder"

./_build-production.sh

cd ${EXEC_DIR}

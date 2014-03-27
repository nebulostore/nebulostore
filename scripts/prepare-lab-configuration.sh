#!/bin/bash

if [ -z "$1" ]; then
  echo "You have to provide param --  x for appKey=xx"
  exit 1
fi

EXEC_DIR=$(pwd)
cd $(dirname $0)

HOST_NUM=$1

APP_KEY=$1$1

# naan address and ports
BOOTSTRAP_ADDRESS=193.0.109.30
BOOTSTRAP_PORT=10201


./_update-xml.sh ../resources/conf/Peer.xml.template \
                 ../resources/conf/LabPeer.xml.template > \
                 ../Peer.xml.base

../resources/conf/generate_config.py \
                      --app-key=$APP_KEY \
                      --bootstrap-net-address=$BOOTSTRAP_ADDRESS \
                      --bootstrap-port=$BOOTSTRAP_PORT \
                      < ../Peer.xml.base > \
                      ../Peer.xml.$1

mv ../Peer.xml.$1 ../build/jar/$1/resources/conf/Peer.xml

cd $EXEC_DIR
       

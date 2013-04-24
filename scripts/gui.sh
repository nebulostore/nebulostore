#!/bin/bash

# Instructions:
#
# 1. Run scripts/gui.sh from 'trunk' level.
# 2. You can run up to 4 local instances.
#    On terminal nr i (1,2,3 or 4) run: (the app key will be 11, 22, etc.)
#       cd build/jar/i/
#       java -jar Nebulostore.jar
# 3. Wait 40 sec (!) for all peers to find each other.

JAR_DIR="build/jar"
PEERS_NUM=4
COMMON_ARGS="--CLASS_NAME=org.nebulostore.gui.GUIController --BOOTSTRAP_ADDRESS=localhost --BOOTSTRAP_TOMP2P_PORT=10301 --BOOTSTRAP_PORT=10201"

./scripts/_build-and-deploy.sh $PEERS_NUM

platform='unknown'
unamestr=`uname`
if [[ "$unamestr" == 'Darwin' ]]; then
  platform='mac'
else
  platform='linux'
fi

sequence='unknown'
if [[ $platform == 'mac' ]]; then
  sequence=`jot $PEERS_NUM`
else
  sequence=`seq 1 $PEERS_NUM`
fi

cd $JAR_DIR
for i in $sequence
do
    cd $i/resources/conf
    ./generate_config.py $COMMON_ARGS --APP_KEY=$i$i --BOOTSTRAP_MODE=client --CLI_PORT=1010$i --TOMP2P_PORT=1030$i --BDB_TYPE=proxy < Peer.xml.template > Peer.xml
    cd ../../../
done

cd 1/resources/conf
./generate_config.py $COMMON_ARGS --APP_KEY=11 --BOOTSTRAP_MODE=server --CLI_PORT=10101 --TOMP2P_PORT=10301 --BDB_TYPE=storage-holder < Peer.xml.template > Peer.xml
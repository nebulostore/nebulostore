#!/bin/bash

# Instructions:
#
# 1. Run scripts/gui.sh from 'trunk' level.
# 2. You can run up to 4 local instances.
#    On terminal nr i (1,2,3 or 4) run: (the app key will be 11, 22, etc.)
#       cd build/jar/i/
#       java -jar Nebulostore.jar
# 3. Wait 40 sec (!) for all peers to find each other.

EXEC_DIR=$(pwd)
cd $(dirname $0)

source _utils.sh

PEERNAME="org.nebulostore.gui.GUIController"
PEERCONF="org.nebulostore.gui.GUIConfiguration"
JAR_DIR="../build/jar"
PEER_NUM=4

./_build-and-deploy.sh -p $PEER_NUM -m peer

# Generate and copy config files.
./_generate-config-files.sh -p $PEERNAME -c $PEERCONF -n $PEER_NUM -m $((PEER_NUM-1)) -b localhost

for ((i=1; i<=$PEER_NUM; i++))
do
    mv ../Peer.xml.$i ../build/jar/$i/resources/conf/Peer.xml
done

cd ${EXEC_DIR}

#!/bin/bash

# Instructions:
#
# 1. Run scripts/console-test.sh from 'trunk' level.
# 2. This script runs (by default) 4 interactive terminals on the localhost
# 3. Play with interactive terminals, for example:
#    On terminal 2:
#       write 33 123 whatever in the file
#    On terminal 1:
#       read 33 123 
#       (expected output: "whetever in the file")
# 4. To exit interactive terminals, 
#    type: exit
#    wait for some time
#    hit enter when prompted


PEER_NUM=4

EXEC_DIR=$(pwd)
cd $(dirname $0)

source _utils.sh

JAR_DIR="../build/jar"
PEERNAME="org.nebulostore.systest.textinterface.TextInterface"
PEERCONF="org.nebulostore.systest.textinterface.TextInterfaceConfiguration"
BOOTSTRAP_SLEEP=2

echo "["`date +"%T"`"] BUILDING ..."
./_build-and-deploy.sh -p $PEER_NUM
if [ $? != 0 ]; then
    echo "BUILD FAILED!"
    exit 1
fi

echo "["`date +"%T"`"] COPYING ..."
./_generate-config-files.sh -p $PEERNAME -c $PEERCONF -n $PEER_NUM\
    -m $PEER_NUM -b localhost

for ((i=1; i<=$PEER_NUM; i++))
do
    mv ../Peer.xml.$i ../build/jar/$i/resources/conf/Peer.xml
done

echo "["`date +"%T"`"] RUNNING INTERACTIVE TERMINALS ..."
cd ${EXEC_DIR}

BOOTSTRAP=true
echo "["`date +"%T"`"] STARTING UI PEERS ..."
for ((i=1; i<=$PEER_NUM; i++))
do
    cd build/jar/$i
    execInNewTerminalWindow "java -jar Nebulostore.jar"
    cd ${EXEC_DIR}
    if [ "$BOOTSTRAP" = true ] ; then
        sleep $BOOTSTRAP_SLEEP
        BOOTSTRAP=false
        echo "["`date +"%T"`"] finished bootstrap sleep"
    fi
done

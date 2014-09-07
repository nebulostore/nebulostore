#!/bin/bash

# Automatic local N-peer test.
# Optional parameters: peer_class_name peer_configuration_class_name
#   test_server_class_name number_of_peers number_of_iterations data_file
# Prints "SUCCESS" or "FAILURE"

# Assuming we are in scripts directory.

source ./_utils.sh

PEERNAME="org.nebulostore.systest.TestingPeer"
PEERCONF="org.nebulostore.systest.TestingPeerConfiguration"
TESTNAME="org.nebulostore.systest.pingpong.PingPongServer"
PEER_NUM=3
TEST_ITER=3
BOOTSTRAP_DELAY=3
LOG_DIR=logs
DATA_FILE=test.data
CONFIG_UPDATE=""

if [ $1 ]; then
  PEERNAME=$1
  PEERCONF=$2
  TESTNAME=$3
  PEER_NUM=$4
  TEST_ITER=$5
  DATA_FILE=${6-'test.data'}
  CONFIG_UPDATE=$7
fi

# Build peers.
echo "BUILDING ..."
./_build-and-deploy.sh -p $PEER_NUM > build.log
if [ $? != 0 ]; then
    echo "BUILD FAILED!"
    exit 1
fi

# Generate and copy config files.
./_generate-config-files.sh -p $PEERNAME -c $PEERCONF -t $TESTNAME -n $PEER_NUM\
    -m $((PEER_NUM-1)) -i $TEST_ITER -b localhost `concatIfNotEmpty -d $DATA_FILE`\
    `concatIfNotEmpty -f $CONFIG_UPDATE`

for ((i=1; i<=$PEER_NUM; i++))
do
    mv ../Peer.xml.$i ../build/jar/$i/resources/conf/Peer.xml
done


# Run server normally and clients in background.
echo "RUNNING ..."
cd ../build/jar

run_clients() {
    sleep $BOOTSTRAP_DELAY
    for ((i=2; i<=$PEER_NUM; i++))
    do
        cd $i
        java -jar Nebulostore.jar > logs/stdout.log 2> logs/stderr.log &
        cd ..
    done
}
run_clients &

EXIT_CODE=0
cd 1
java -jar Nebulostore.jar > logs/stdout.log 2> logs/stderr.log
if [ $? -ne 0 ]; then
  EXIT_CODE=1
fi
cd ../../..

# Copy logs.
rm -rf $LOG_DIR
mkdir $LOG_DIR
echo "COPYING LOGS ..."
for ((i=1; i<=$PEER_NUM; i++))
do
    cp -r build/jar/$i/logs $LOG_DIR/logs_$i
done

# Kill remaining peers.
kill `ps -eo pid,command | grep Nebulostore | grep -v grep | awk '{ print $1 }'`
if [ $EXIT_CODE -eq 0 ]
then
    echo "SUCCESS"
else
    echo "FAILURE"
fi

cd scripts
exit $EXIT_CODE

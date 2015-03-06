#!/bin/bash

SCRIPT_NAME=./_local-test.sh
declare -a PEERS=(3 16 6 6 8 14 6 3 3 7)
N_TESTS=${#PEERS[@]}
declare -a TITLES=(\
    'basic ping-pong test'\
    'ping-pong test'\
    'read-write test'\
    'lists test'\
    'performance lists test'\
    'performance lists test'\
    'read-write time measure test'\
    'network monitor test'\
    'broker test'\
    'asynchronous messages test'\
    )

EXEC_DIR=$(pwd)
cd $(dirname $0)


if [ $1 ]; then
    N=$1
else
    echo "Please select local test to run (or provide the number as a parameter):"
    echo "    0) All"
    for ((i=1; i<=N_TESTS; ++i))
    do
        echo "    $i) "${TITLES[$((i-1))]}" (${PEERS[$((i-1))]} peers)"
    done
    read N
fi

case $N in
    0) cd $EXEC_DIR
       for ((i=1; i<=$N_TESTS; ++i))
       do
           echo "*** Test $i - ${TITLES[$((i-1))]}"
           $0 $i
           exit_code=$?
           if [ "$exit_code" -ne 0 ]; then
               echo "*** TEST $i FAILED; exiting;"
               exit $exit_code
           fi
       done
       cd $(dirname $0)
       ;;
    1) $SCRIPT_NAME\
           org.nebulostore.systest.TestingPeer\
           org.nebulostore.systest.TestingPeerConfiguration\
           org.nebulostore.systest.basictest.BasicPingPongServer\
           ${PEERS[0]} 1;;
    2) $SCRIPT_NAME\
           org.nebulostore.systest.TestingPeer\
           org.nebulostore.systest.TestingPeerConfiguration\
           org.nebulostore.systest.pingpong.PingPongServer\
           ${PEERS[1]} 1;;
    3) $SCRIPT_NAME\
           org.nebulostore.systest.TestingPeer\
           org.nebulostore.systest.TestingPeerConfiguration\
           org.nebulostore.systest.readwrite.ReadWriteServer\
           ${PEERS[2]} 1;;
    4) $SCRIPT_NAME\
           org.nebulostore.systest.TestingPeer\
           org.nebulostore.systest.TestingPeerConfiguration\
           org.nebulostore.systest.lists.ListsServer\
           ${PEERS[3]} 1;;
    5) $SCRIPT_NAME\
           org.nebulostore.systest.performance.PerfTestingPeer\
           org.nebulostore.systest.performance.PerfTestingPeerConfiguration\
           org.nebulostore.systest.lists.ListsServer\
           ${PEERS[4]} 1;;
    6) $SCRIPT_NAME\
           org.nebulostore.systest.performance.PerfTestingPeer\
           org.nebulostore.systest.performance.PerfTestingPeerConfiguration\
           org.nebulostore.systest.lists.ListsServer\
           ${PEERS[5]} 1;;
    7) $SCRIPT_NAME\
           org.nebulostore.systest.TestingPeer\
           org.nebulostore.systest.readwrite.ReadWriteWithTimeConfiguration\
           org.nebulostore.systest.readwrite.ReadWriteServer\
           ${PEERS[6]} 1 ../../../test.data;;
    8) $SCRIPT_NAME\
           org.nebulostore.systest.TestingPeer\
           org.nebulostore.systest.TestingPeerConfiguration\
           org.nebulostore.systest.networkmonitor.NetworkMonitorTestServer\
           ${PEERS[7]} 1 test.data ../nebulostore/src/main/resources/systest/broker-test-1.xml;;
    9) $SCRIPT_NAME\
           org.nebulostore.systest.TestingPeer\
           org.nebulostore.systest.TestingPeerConfiguration\
           org.nebulostore.systest.broker.BrokerTestServer\
           ${PEERS[8]} 1 test.data ../nebulostore/src/main/resources/systest/broker-test-1.xml;;
    10) $SCRIPT_NAME\
           org.nebulostore.systest.async.AsyncTestingPeer\
           org.nebulostore.systest.async.AsyncTestingPeerConfiguration\
           org.nebulostore.systest.async.AsyncTestServer\
           ${PEERS[9]} 1;;
esac
EXIT_CODE=$?

cd ${EXEC_DIR}
exit $EXIT_CODE

#!/bin/bash

SCRIPT_NAME=./_planet-lab-test.sh
HOSTS_FILE=nodes/hosts.txt

EXEC_DIR=$(pwd)
cd $(dirname $0)

if [ $1 ]; then
    N=$1
else
    echo "Please select PlanetLab test to run (or provide the number as a parameter):"
    echo "    1) basic ping-pong test (3 peers / 2 test clients)"
    echo "    2) ping-pong test (16 peers / 15 test clients)"
    echo "    3) read-write test (7 peers / 5 test clients)"
    echo "    4) lists test (7 peers / 5 test clients)"
    echo "    5) performance lists test (11 peers / 8 test clients)"
    echo "    6) performance lists test (30 peers / 25 test clients)"
    echo "    7) performance lists test (60 peers / 50 test clients)"
    echo "    8) performance lists test (95 peers / 80 test clients)"
    echo "    9) performance lists test (120 peers / 100 test clients)"
    echo "    10) asynchronous messages test (7 peers / 6 test clients)"
    read N
fi

case $N in
    1) $SCRIPT_NAME\
           org.nebulostore.systest.TestingPeer\
           org.nebulostore.systest.TestingPeerConfiguration\
           org.nebulostore.systest.basictest.BasicPingPongServer\
           3 2 1\
           $HOSTS_FILE;;
    2) $SCRIPT_NAME\
           org.nebulostore.systest.TestingPeer\
           org.nebulostore.systest.TestingPeerConfiguration\
           org.nebulostore.systest.pingpong.PingPongServer\
           16 15 1\
           $HOSTS_FILE;;
    3) $SCRIPT_NAME\
           org.nebulostore.systest.TestingPeer\
           org.nebulostore.systest.TestingPeerConfiguration\
           org.nebulostore.systest.readwrite.ReadWriteServer\
           7 5 1\
           $HOSTS_FILE;;
    4) $SCRIPT_NAME\
           org.nebulostore.systest.TestingPeer\
           org.nebulostore.systest.TestingPeerConfiguration\
           org.nebulostore.systest.lists.ListsServer\
           7 5 1\
           $HOSTS_FILE;;
    5) $SCRIPT_NAME\
           org.nebulostore.systest.performance.PerfTestingPeer\
           org.nebulostore.systest.performance.PerfTestingPeerConfiguration\
           org.nebulostore.systest.lists.ListsServer\
           11 8 1\
           $HOSTS_FILE;;
    6) $SCRIPT_NAME\
           org.nebulostore.systest.performance.PerfTestingPeer\
           org.nebulostore.systest.performance.PerfTestingPeerConfiguration\
           org.nebulostore.systest.lists.ListsServer\
           30 25 1\
           $HOSTS_FILE;;
    7) $SCRIPT_NAME\
           org.nebulostore.systest.performance.PerfTestingPeer\
           org.nebulostore.systest.performance.PerfTestingPeerConfiguration\
           org.nebulostore.systest.lists.ListsServer\
           60 50 1\
           $HOSTS_FILE;;
    8) $SCRIPT_NAME\
           org.nebulostore.systest.performance.PerfTestingPeer\
           org.nebulostore.systest.performance.PerfTestingPeerConfiguration\
           org.nebulostore.systest.lists.ListsServer\
           95 80 1\
           $HOSTS_FILE;;
    9) $SCRIPT_NAME\
           org.nebulostore.systest.performance.PerfTestingPeer\
           org.nebulostore.systest.performance.PerfTestingPeerConfiguration\
           org.nebulostore.systest.lists.ListsServer\
           120 100 1\
           $HOSTS_FILE;;
    10) $SCRIPT_NAME\
           org.nebulostore.systest.async.AsyncTestingPeer\
           org.nebulostore.systest.async.AsyncTestingPeerConfiguration\
           org.nebulostore.systest.async.AsyncTestServer\
           7 6 1\
           $HOSTS_FILE;;
esac

cd ${EXEC_DIR}

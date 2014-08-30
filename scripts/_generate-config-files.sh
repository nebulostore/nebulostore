#!/bin/bash

# Generate config files for test peers.
# Uses config template from ./resources/conf/Peer.xml.template updated
# with parameters from CONFIG_TEMPLATE_PATCH XML file (if set) and
# parameters from the command line.
#
# First peer is bootstrap server.
# Last peer is testing peer.
# Optionally, the following parameters might be provided:
#   -p peer_class_name
#   -c configuration_class_name
#   -t test_class_name
#   -n number_of_peers
#   -m number_of_test_clients
#   -i number_of_test_iterations
#   -b bootstrap_address/naming_server_net_address
#   -h host_list
#   -v comm_module_version
#   -d [data_file]
#   -f [config_patch_file]
#   -r rest_api_port_default

EXEC_DIR=$(pwd)
cd $(dirname $0)

while getopts ":p:c:t:n:m:i:b:d:h:v:f:r:" OPTION
do
  case $OPTION in
    p) PEERNAME=$OPTARG;;
    c) PEERNAME_CONFIGURATION=$OPTARG;;
    t) TESTNAME=$OPTARG;;
    n) PEER_NUM=$OPTARG;;
    m) TEST_CLIENTS_NUM=$OPTARG;;
    i) TEST_ITER=$OPTARG;;
    b) BOOTSTRAP_ADDRESS=$OPTARG
       REMOTEMAP_SERVER_NET_ADDRESS=$OPTARG;;
    d) DATA_FILE=$OPTARG;;
    h) HOST_LIST=$OPTARG;;
    v) COMM_MODULE=$OPTARG;;
    f) CONFIG_TEMPLATE_PATCH=$OPTARG;;
    r) REST_API_PORT_DEFAULT=$OPTARG;;
    # DEFAULT
    *)
       ARG=$(($OPTIND-1));
       if [ ${!ARG} = "-d" ]
       then
           DATA_FILE=""
       else
           echo "Unknown option option chosen: ${!ARG}.";
       fi
  esac
done

: ${PEERNAME="org.nebulostore.systest.TestingPeer"}
: ${PEERNAME_CONFIGURATION="org.nebulostore.systest.TestingPeerConfiguration"}
: ${TESTNAME="org.nebulostore.systest.pingpong.PingPongServer"}
: ${PEER_NUM=3}
: ${TEST_CLIENTS_NUM=2}
: ${TEST_ITER=3}
: ${BOOTSTRAP_ADDRESS="localhost"}
: ${BOOTSTRAP_COMM_ADDRESS=00000000-0000-0000-0001-000000000000}
: ${REMOTEMAP_LOCAL_PORT=1101}
: ${REMOTEMAP_SERVER_NET_ADDRESS="localhost"}
: ${NAMING_SERVER_NET_ADDRESS="localhost"}
: ${COMMON_ARGS="--bootstrap-port=10001 --bootstrap-server-tomp2p-port=12001"}
: ${COMM_MODULE="newcommunication"}
: ${DATA_FILE=test.data}
: ${CONFIG_TEMPLATE_PATCH=""}

# Generate test list.
for ((i=1; i<=$TEST_ITER; i++))
do
    if [ $i -ne 1 ]
    then
        CONCAT=$CONCAT;
    fi
    CONCAT=$CONCAT$TESTNAME
done

#generate host address array
if [[ -n $HOST_LIST ]]; then
    i=1
    for host in `cat $HOST_LIST`; do
        CLIENT_HOSTS[$i]=$host
        if [ $i -eq $PEER_NUM ]; then break; else ((i++)); fi
    done
fi

if [ $CONFIG_TEMPLATE_PATCH ]; then
  ./_update-xml.sh ../resources/conf/Peer.xml.template $CONFIG_TEMPLATE_PATCH > ../Peer.xml.base
else
  cp ../resources/conf/Peer.xml.template ../Peer.xml.base
fi

# Configure peers.
for ((i=1; i<=$PEER_NUM; i++))
do
    # Make first peer bootstrap server, storage holder and test server.
    if [ $i -eq 1 ]
    then
        BOOTSTRAP_MODE="server"
        BDB_TYPE="storage-holder"
        IS_SERVER="true"
        REMOTEMAP_MODE="server"
    else
        BOOTSTRAP_MODE="client"
        BDB_TYPE="proxy"
        IS_SERVER="false"
        REMOTEMAP_MODE="client"
    fi

    if [[ -n $CLIENT_HOSTS[$i] ]]; then
        LOCAL_NET_ADDR=${CLIENT_HOSTS[$i]}
    fi

    PADDED=`printf "%03d" $i`

    if [[ -z "$REST_API_PORT_DEFAULT" ]]; then
        REST_API_PORT=14$PADDED
    else
        REST_API_PORT=$REST_API_PORT_DEFAULT
    fi

    ../resources/conf/generate_config.py $COMMON_ARGS \
         --app-key=$i$i \
         --class-name=$PEERNAME \
         --configuration-class-name=$PEERNAME_CONFIGURATION \
         --testing-peer-class-list=$CONCAT\
         --local-net-address=$LOCAL_NET_ADDR \
         --bootstrap-net-address=$BOOTSTRAP_ADDRESS \
         --bootstrap/mode=$BOOTSTRAP_MODE \
         --bootstrap-comm-address=$BOOTSTRAP_COMM_ADDRESS \
         --comm-module=$COMM_MODULE \
         --comm-address=00000000-0000-0000-0$PADDED-000000000000 \
         --comm-cli-port=11$PADDED --tomp2p-port=12$PADDED\
         --bdb-peer/type=$BDB_TYPE\
         --bootstrap/address=$BOOTSTRAP_ADDRESS \
         --remotemap/mode=$REMOTEMAP_MODE \
         --remotemap/local-port=$REMOTEMAP_LOCAL_PORT \
         --remotemap/server-net-address=$REMOTEMAP_SERVER_NET_ADDRESS \
         --remotemap/server-port=$REMOTEMAP_LOCAL_PORT \
         --systest/is-server=$IS_SERVER\
         --num-test-participants=$TEST_CLIENTS_NUM\
         --systest/data-file=$DATA_FILE \
         --rest-api/server-config/port=$REST_API_PORT\
         --bdb-peer/holder-comm-address=00000000-0000-0000-0001-000000000000 < ../Peer.xml.base > ../Peer.xml.$i
done

mv ../Peer.xml.base ../build/Peer.xml.base

cd ${EXEC_DIR}

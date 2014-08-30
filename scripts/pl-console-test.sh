#!/bin/bash

# Instructions:
#
# 1. run scripts/pl-console-test.sh from 'trunk' level
# 2. This script runs (by default) 3 interactive terminals on planet-lab nodes
#    and one non-interactive peer
# 3. Play on the interactive terminals, for example:
#    On terminal 2:
#       write 33 123 whatever in the file
#    On terminal 1:
#       read 33 123
#       (expected output: "whetever in the file")
# 4. To exit interactive terminals,
#    type: exit
#    wait for some time
#    hit enter when prompted
# 5. To exit non-interactive terminals, run scripts/killall-java.sh

PEER_NUM=4
UI_PEER_NUM=3

EXEC_DIR=$(pwd)
cd $(dirname $0)

. _constants.sh
. _utils.sh

PEERNAME="org.nebulostore.systest.textinterface.TextInterface"
PEERCONF="org.nebulostore.systest.textinterface.TextInterfaceConfiguration"
NON_UI_PEER_NUM=`expr $PEER_NUM - $UI_PEER_NUM`
HOST_LIST="nodes/hosts.txt"
BOOTSTRAP_PEER=`head -1 $HOST_LIST`
MAX_THREADS=1
BOOTSTRAP_SLEEP=5
LOG_DIR=logs

echo "["`date +"%T"`"] BUILDING ..."
./_build-and-deploy.sh -p 1 -m peer > /dev/null
if [ $? != 0 ]; then
    echo "BUILD FAILED!"
    exit 1
fi
./_generate-config-files.sh -p $PEERNAME -c $PEERCONF -t "ignored-for-console" -n $PEER_NUM\
    -m 0 -i 0 -b $BOOTSTRAP_PEER -h $HOST_LIST -r 14000


echo "["`date +"%T"`"] COPYING ..."
i=1
PAIRS=""
CLIENT_HOSTS=""
for host in `cat $HOST_LIST`
do
    PAIRS+="$i $host "
    if [ $i -ne 1 ]; then CLIENT_HOSTS+="$host "; fi
    if [ $i -eq $PEER_NUM ]; then break; else ((i++)); fi
done
echo $PAIRS | xargs -P $MAX_THREADS -n 2 ./_pl-deploy-single.sh

BOOTSTRAP=true
echo "["`date +"%T"`"] STARTING UI PEERS ..."
for host in `head -$UI_PEER_NUM $HOST_LIST`
do
    echo "["`date +"%T"`"] host: $host"
    execInNewTerminalWindow "ssh -o $SSH_OPTIONS -l $USER $host \"hostname; cd $REMOTE_DIR; $JAVA_EXEC -jar Nebulostore.jar\"; echo hit enter to close the window; read"
    if [ "$BOOTSTRAP" = true ] ; then
        sleep $BOOTSTRAP_SLEEP
        BOOTSTRAP=false
        echo "["`date +"%T"`"] finished bootstrap sleep for $host"
    fi
done

echo "["`date +"%T"`"] STARTING NON-UI PEERS ..."
first_non_ui=`expr $UI_PEER_NUM + 1`
for host in `tail -n +$first_non_ui $HOST_LIST | head -$NON_UI_PEER_NUM`
do
    echo "["`date +"%T"`"] host: $host"
    ssh -o $SSH_OPTIONS -l $USER $host "cd $REMOTE_DIR; $JAVA_EXEC -jar Nebulostore.jar > logs/stdout.log 2>logs/stderr.log &"
done

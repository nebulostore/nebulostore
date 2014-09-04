#!/bin/bash

# Uploads dependency libraries to nebulostore_autodeploy folder on hosts listed
# in nodes/hosts.txt file.

# Usage:
# bash upload-libs-to-planet-lab.sh [-n NUMBER_OF_HOSTS=200] [-v]
#   -v sets the verbose flag

EXEC_DIR=$(pwd)
cd $(dirname $0)

. _constants.sh
REMOTE_DIR="nebulostore_autodeploy/"
HOST_LIST="nodes/hosts.txt"
JAVA_HOST="roti.mimuw.edu.pl"
JAVA_DIR_TMP="tmp/"

while getopts ":n:v" OPTION
do
  case $OPTION in
    n) N=$OPTARG;;
    v) RSYNC_OPTIONS="-rlvu --size-only";;
    *) ARG=$(($OPTIND-1))
       echo "Unknown option option chosen: ${!ARG}."
  esac
done

: ${N=200}
SSH_OPTIONS="StrictHostKeyChecking=no"
: ${RSYNC_OPTIONS="-rlu --size-only"}

echo "BUILDING ..."
./_build-and-deploy.sh -p 1 -m peer > /dev/null

echo "COPYING ..."
i=1
for HOST in $(cat $HOST_LIST | head -n $N)
do
    echo "  "$((i++))": ["`date +"%T"`"]" $HOST
    rsync -e "ssh -o $SSH_OPTIONS"\
        $RSYNC_OPTIONS ../build/jar/lib $USER@$HOST:~/$REMOTE_DIR/
done

JAVA_DIR=`dirname $JAVA_EXEC`
JAVA_DIR=`dirname $JAVA_DIR`
echo "Getting java from $JAVA_HOST directory $JAVA_DIR to $JAVA_DIR_TMP"
rsync -e "ssh -o $SSH_OPTIONS"\
         $RSYNC_OPTIONS $USER@$JAVA_HOST:$JAVA_DIR ../$JAVA_DIR_TMP

echo "Copying java to hosts"
for HOST in $(cat $HOST_LIST | head -n $N)
do
    echo "  "$((i++))": ["`date +"%T"`"]" $HOST
    rsync -e "ssh -o $SSH_OPTIONS"\
        $RSYNC_OPTIONS ../$JAVA_DIR_TMP $USER@$HOST:
done

cd ${EXEC_DIR}

#!/bin/bash

EXEC_DIR=$(pwd)
cd $(dirname $0)
. _constants.sh
. _jar-properties.sh

echo "   "$2
ssh -o $SSH_OPTIONS -l $USER $2 "mkdir -p $REMOTE_DIR; rm -rf $REMOTE_DIR/logs* $REMOTE_DIR/storage* $REMOTE_DIR/resources*"
rsync -rul ../build/jar/1/* $USER@$2:~/$REMOTE_DIR/
rsync -rul ../build/jar/lib/$JAR_NAME $USER@$2:~/$REMOTE_DIR/lib
rsync -rul ../Peer.xml.$1 $USER@$2:~/$REMOTE_DIR/resources/conf/Peer.xml
rm ../Peer.xml.$1

cd ${EXEC_DIR}
exit 0

#!/bin/bash

# Build and copy PEERS_NUM peers with resources to JAR_DIR/i/.
# Optional parameters: number_of_peers maven_profile_name

PEERS_NUM=4
MAVEN_JAR="target/nebulostore-0.4.jar"
MAVEN_LIB="target/lib"
MAVEN_TARGET="peer"
BUILD_DIR="build"
JAR_DIR="build/jar"
JAR="Nebulostore.jar"

if [ $1 ]; then
  PEERS_NUM=$1
fi

if [ $2 ]; then
  MAVEN_TARGET=$2
fi

rm -rf $BUILD_DIR
mvn clean install -P $MAVEN_TARGET

echo "Building done. Copying..."

for i in `seq 1 $PEERS_NUM`
do
    echo $i
    CURR_PATH="./$JAR_DIR/$i"
    mkdir -p $CURR_PATH
    mkdir -p $CURR_PATH/logs
    mkdir -p $CURR_PATH/storage/bdb
    cp $MAVEN_JAR $CURR_PATH/$JAR
    rsync -r $MAVEN_LIB $CURR_PATH/
    rsync -r --exclude=.svn resources $CURR_PATH
done


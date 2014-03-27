#!/bin/bash

# Build and copy PEERS_NUM peers with resources to JAR_DIR/i/.
# Optional parameters: -p number_of_peers -m maven_profile_name
EXEC_DIR=$(pwd)
cd $(dirname $0)

source _jar-properties.sh
source _utils.sh

while getopts ":p:m:" OPTION
do
  case $OPTION in
    p) PEERS_NUM=$OPTARG;;
    m) MAVEN_TARGET=$OPTARG;;
   # DEFAULT
   *)
       ARG=$(($OPTIND-1)); echo "Unknown option option chosen: ${!ARG}.";
  esac
done

: ${PEERS_NUM=4}
MAVEN_JAR="../nebulostore-systest/target/$SYSTEST_JAR_NAME"
MAVEN_LIB="../nebulostore-systest/target/lib"
: ${MAVEN_TARGET="peer"}
BUILD_DIR="../build"
JAR_DIR="../build/jar"
JAR="Nebulostore.jar"

rm -rf $BUILD_DIR
buildNebulostore $MAVEN_TARGET

echo "Building done. Copying..."

rm -rf $JAR_DIR
mkdir -p $JAR_DIR
rsync -r $MAVEN_LIB $JAR_DIR/
for ((i=1; i<=$PEERS_NUM; i++))
do
    echo $i
    CURR_PATH="$JAR_DIR/$i"
    createNebuloLocalArtifact $CURR_PATH $MAVEN_JAR $JAR
done

cd $EXEC_DIR


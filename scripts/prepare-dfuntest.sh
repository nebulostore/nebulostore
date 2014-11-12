#!/bin/bash
# Build dfuntest library and environment and put them into build directory.
EXEC_DIR=$(pwd)
cd $(dirname $0)

source _jar-properties.sh
source _utils.sh

MAVEN_JAR="../nebulostore-dfuntest/target/$DFUNTEST_JAR_NAME"
MAVEN_LIB="../nebulostore-dfuntest/target/lib"
BUILD_DIR="../build"
JAR="Nebulostore.jar"

rm -rf $BUILD_DIR
buildNebulostore
if [ $? != 0 ]; then
    echo "BUILD FAILED!"
    exit 1
fi

echo "Building done. Copying..."
mkdir -p $BUILD_DIR
createNebuloLocalArtifact $BUILD_DIR $MAVEN_JAR $JAR
rsync -r $MAVEN_LIB $BUILD_DIR/

cp ../resources/conf/Peer.xml.template $BUILD_DIR/resources/conf/Peer.xml

cd ${EXEC_DIR}

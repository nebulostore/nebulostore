#!/bin/bash

# Builds ping-pong-server(in build/jar/server) and client and bootstrap server.
# Run them both to perform a test.

BUILD_DIR="build"
JAR_DIR="build/jar"
JAR_DIR_SERVER="$JAR_DIR/server"
JAR="Nebulostore.jar"
PEERS_NUM=1

platform='unknown'
unamestr=`uname`
if [[ "$unamestr" == 'Darwin' ]]; then
  platform='mac'
else
  platform='linux'
fi

sequence='unknown'
#if [[ $platform=='mac' ]]; then
#  sequence=`jot $PEERS_NUM`
#else
sequence=`seq 1 $PEERS_NUM`
#fi

rm -rf $BUILD_DIR
ant test-server

mkdir ./$JAR_DIR_SERVER
cp ./$JAR_DIR/*.jar ./$JAR_DIR_SERVER/
cp -r ./$JAR_DIR/lib ./$JAR_DIR_SERVER/
cp -r resources ./$JAR_DIR_SERVER/

echo "Building and copying server done."

ant test-client

echo "Building clients done. Copying..."

echo 1
path="./$JAR_DIR/1"
mkdir $path
cp ./$JAR_DIR/*.jar ./$JAR_DIR/1/
cp -r ./$JAR_DIR/lib ./$JAR_DIR/1/
cp -r resources ./$JAR_DIR/1/
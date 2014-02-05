#!/bin/bash

#param1 path to destination directory
function _createDistributionDirectory() {
    rm -rf $1
    mkdir -p $1
    mkdir -p $1/logs
    mkdir -p $1/storage/bdb
}

function _copyLibraries() {
    cp -r ../target/lib $1/lib
}

function _linkLibraries() {
    ln -s ../lib $1/lib
}

function _copyNebuloJar() {
    cp $2 $1/$3
}

function _copyConfig {
    rsync -r --exclude=.svn ../resources $1
    rm -rf $1/resources/checkstyle
    rm $1/resources/conf/Peer.xml.template
    rm $1/resources/conf/generate_config.py
}

function generateConfigFile() {
    ../resources/conf/generate_config.py $1 < ../resources/conf/Peer.xml.template > $2/Peer.xml
}

function buildNebulostore() {
    EXEC_DIR=$(pwd)
    cd ../
    mvn clean install -P$1
    cd $EXEC_DIR
}

function generateReadMe() {
    cp ../README $1/README
}

function addLicenseFile() {
    cp ../LICENSE $1/LICENSE
}

#param1 path to destination directory
#param2 nebulostore jar path
#param3 nebulostore jar final name
function createNebuloProductionArtifact() {
    _createDistributionDirectory $1
    _copyLibraries $1
    _copyNebuloJar $1 $2 $3
    _copyConfig $1
    generateReadMe $1
    addLicenseFile $1
}

#param1 path to destination directory
function createNebuloLocalArtifact() {
    _createDistributionDirectory $1
    _linkLibraries $1
    _copyNebuloJar $1 $2 $3
    _copyConfig $1
    generateReadMe $1
    addLicenseFile $1
}

function compressDistributionDir() {
    EXEC_DIR=$(pwd)
    cd $1
    tar czf $3.tar.gz $2
    cd ${EXEC_DIR}
}

function concatIfNotEmpty() {
    if [ -z $1 ]
    then
        echo ""
        return 0
    fi
    if [ -z $2 ]
    then
        echo ""
        return 0
    fi
    echo "$1 $2"
}

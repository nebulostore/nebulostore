#!/bin/bash
# Executes dfuntest's tests after preparing by scripts/prepare-dfuntest.sh
EXEC_DIR=$(pwd)
cd $(dirname $0)
cd ../build/

java -cp Nebulostore.jar:lib/* org.nebulostore.dfuntest.NebulostoreDfuntestMain\
 --env-factory local --config-file resources/conf/nebulostoredfuntest.xml

cd ${EXEC_DIR}

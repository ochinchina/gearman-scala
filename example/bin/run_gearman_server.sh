#!/bin/sh

CUR_DIR=`dirname $0`
cd $CUR_DIR/../..
java -cp build/libs/gearman-scala-all-1.1.jar org.gearman.server.GearmanServer 127.0.0.1 4730

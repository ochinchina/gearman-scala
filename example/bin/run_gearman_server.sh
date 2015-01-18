#!/bin/sh

CUR_DIR=`dirname $0`
cd $CUR_DIR
cd ../..
sbt "run-main org.gearman.server.GearmanServer 4730"

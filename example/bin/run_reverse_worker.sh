#!/bin/sh

CUR_DIR=`dirname $0`
cd $CUR_DIR
cd ..
sbt "run-main org.gearman.example.reverse.ReverseWorker 127.0.0.1:4730"

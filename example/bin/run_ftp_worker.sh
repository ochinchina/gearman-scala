#!/bin/sh

CUR_DIR=`dirname $0`
cd $CUR_DIR/..
java -cp build/libs/example-all.jar org.gearman.example.ftp.FtpWorker 127.0.0.1:4730

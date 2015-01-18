#!/bin/sh

CUR_DIR=`dirname $0`
cd $CUR_DIR
CUR_DIR=`pwd`
cd ..
sbt "run-main org.gearman.example.chat.AsyncChatWorker 127.0.0.1:4730"

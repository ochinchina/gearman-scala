#!/bin/sh

CUR_DIR=`dirname $0`
cd $CUR_DIR/..
#gradle fatJar
java -cp build/libs/example-all.jar org.gearman.example.chat.ChatClient 127.0.0.1:4730

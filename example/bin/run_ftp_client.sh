#!/bin/sh

CUR_DIR=`dirname $0`
cd $CUR_DIR
cd ..
sbt "run-main org.gearman.example.ftp.FtpClient 127.0.0.1:4730 build.sbt"

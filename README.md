gearman-scala
=============

gearman implementation in scala including server, client &amp; worker

compile gearman server
======================

* download the sbt tool
* clone this git
* run "sbt test:compile" to compile the gearman server

start gearman server
====================

scala -classpath target/scala-2.11/gearman_2.11-1.0.jar org.gearman.server.GearmanServer <listening ip> <listening port>

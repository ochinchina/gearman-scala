gearman-scala
=============

This is a gearman implementation in scala language. It includes:

* gearman server, gearman server accepts the jobs from client and dispatch them to worker
* gearman client library, used to write application who submit work to the gearman server
* gearman worker library, used to get jobs from the server and process it.

#What's gearman


Please visit gearman wiki http://en.wikipedia.org/wiki/Gearman to get gearman introduction.
For the detailed technical information, please visit http://gearman.org/ 

#Compile gearman


Before using gearman, we need to compile it

* download the sbt tool from sbt website http://www.scala-sbt.org/
* clone the gearman-scala repository

```shell
	$ git clone https://github.com/ochinchina/gearman-scala.git
```

* run "sbt test:compile" to compile the gearman

#A Simple Example

We write a simple "reverse" to demo how to write worker and client in Scala language

##start gearman server

Execute following command to start the gearman server:

```shell
scala -classpath target/scala-2.11/gearman_2.11-1.0.jar org.gearman.server.GearmanServer [listening ip] <listening port>
```

##write & start gearman worker

First the worker need to connect to the gearman server. One worker can connect one
or more gearman server.

```scala
import org.gearman.worker._

//connect two gearman server at same time
val worker = GearmanWorker( "192.168.1.1:4730,192.168.1.2:4730")
```

If the gearman server is not started, the worker will connect to the gearman server
automatically when the gearman server is started.

After constructing a gearman worker, user can tell the gearman server what work this
worker can do by invoking canDo method.
  
```scala

worker.canDo( "reverse" ) { case ( data, uid, responser ) =>
	responser complete reverse( data )
})

def reverse( data: String ): String = new StringBuilder( data ).reverse.toString
```

Before gearman worker exit, it should call cantDo with function name to tell the
gearman server, it will not do the specific work. And the call the shutdown method
to shutdown the worker

```scala
worker.cantDo( "reverse")
worker.shutdown( true )
```

After the worker is ready, start it like below:

```shell
scala -classpath <classpath> yourWorker
```

##write gearman & start client

At first, the gearman client needs to connect with a gearman server. One or more
gearman server can be provided to the Gearman client. But at any time, the gearman
client only connects to one gearman server. If the connected gearman server exit,
the client will automatically connect to another gearman server.

```scala
import org.gearman.client._

//connect to one gearman server
val client = GearmanClient( "192.168.1.1:4730,192.168.1.2:4730")

```

After connecting to the gearman server, the client can ask gearman server dispath
some work to the registred gearman worker.

```scala

//Do string reverse and print the result
client.submitJob( "reverse", "hello, world") {
	case JobComplete( data ) => println( data )
}
```

If client wants to exit, it can call the shutdown method to do the graceful shutdown( let
the on-going work finished)

```scala

//Do string reverse and print the result
client shutdown true
```

After the client is ready, start it like below:

```shell
scala -classpath <classpath> yourWorker
```

The source of this example can be found at example/org.gearman/example/reverse directory

##Another examples

The another examples can be found at  example/org.gearman/example directory

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
```shell
	$ cd gearman-scala
	$ gradle fatJar
	$ java -jar build/libs/gearman-scala-all.jar 4730
```

#A Simple Example

We write a simple "reverse" to demo how to write worker and client in Scala language

##start gearman server

A gearman start script is available in the bin directory, execute following command to start the gearman server:

```shell
bin/run_gearman_server.sh
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

worker.canDo( "reverse" ) { case ( data, uid, responser, dataFetcher ) =>
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
sbt "run-main yourWorker param1 param2..."
```

##write & start client

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
sbt "run-main yourClient param1 param2..."
```
The source of this example can be found at example/src/main/scala/org/gearman/example/reverse directory

##extend gearman protocol

In the current gearman protocol, after submitting a job to the gearman server, the client can't send any data to the job any more. 

This gearman implementation extends the gearman protocol to support bi-direction communication between the client & worker. A chat example shows how the client communicates with worker after submitting a job to gearman server.

Gearman client:
```scala
01 //connect to gearman server
02 val client = GearmanClient( "192.168.1.1:4730,192.168.1.2:4730")
03 val talkingContents = new LinkedList[String]
04 talkingContents.add( "Hello!")
05 talkingContents.add( "How are you?" )
06 talkingContents.add( "What are you doing recently?" )
07 talkingContents.add( "bye!")
08
09 //declare a JobDataSender object
10 var jobDataSender: JobDataSender = null
11
12 //submit a "chat" job to gearman server
13 client.submitJob( "chat", "") {
14    case JobData( data ) =>
15        //send data to worker after receiving a data from worker
16        jobDataSender.data( talkingContents.removeFirst )
17    case JobComplete( data )=>
18 }.onSuccess {
19    //return the job handle and the JobDataSender object after submitting job to gearman server, save the jobDataSender 
20    case (jobHandle, tmpJobDataSender ) => jobDataSender = tmpJobDataSender
21 }
```

Gearman worker in Sync mode:

```scala
//connect to the gearman server
01 val worker = GearmanWorker( "192.168.1.1:4730,192.168.1.2:4730" )
02 //create a chat worker
03 worker.canDo ( "chat") {
04     //after extending the gearman protocol, a dataFetcher object is available to get data from client
05     case ( content, uid, responser, dataFetcher ) =>
06            responser.data( "received:" + content )
07            var stop = false
08            while( !stop ) {
09                //try to get data from the client side
10                dataFetcher.data match {
11                    //if "bye!" is reiceived, complete the job
12                    case "bye!" =>
13                         responser.complete("")
14                         stop = true
15                    case data: String =>
16                          //simply response with received data
17                          responser.data( "received:" + data )
18                }
19            }
20 }

```

Gearman worker in async mode:
```scala
//connect to the gearman server
01 val worker = GearmanWorker( "192.168.1.1:4730,192.168.1.2:4730" )
02 //create a chat worker with callback function
03 worker.canDo ( "chat") ( processChatData) 
04
05 private def processChatData(content:String,uid:Option[String],responser:JobResponser,dataFetcher:JobDataFetcher){
06     content match {
07         case "bye!" =>
08             responser.complete("")
09         case data:String =>
10             responser.data( "received:" + data )
11             //fetch data from the client side in async mode
12             dataFetcher.data( processChatData )
13      }
14 }

```
The complete chat example can be found at example/src/main/scala/org/gearman/example/chat directory

##Other examples

The another examples can be found at  example/src/main/scala/org/gearman/example/ directory

##run the examples
 You try to run the example after installing the sbt and clone the git respository. There are some scripts under example/bin directory to start these examples.
 At first you should start the gearman server:
 
 ```shell
 bin/run_gearman_server.sh
 ```
 
 Then start the worker, for example to start the reverse worker, you can:
 
 ```shell
 example/bin/run_reverse_client.sh
 ```
 
 Finally start the client, for example to start the reverse client, you start it by calling:
 
 ```shell
 example/bin/run_reverse_worker.sh
 ```

#License

Apache License 2

#API document

http://ochinchina.github.io/gearman-scala-api/

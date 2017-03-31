package org.gearman.example.chat

import org.gearman.worker._
import scala.io.Source

/**
 * This is the worker side of "chat" implemented in gearman. It register "chat" 
 * function to the gearman server. When a client submit a "chat" job to the
 * gearman server, this worker will fetch the job from the gearman server and
 * exchange data between the client and the worker.   
 *
 * @author Steven Ou 
 */  
object ChatWorker {
	def main( args: Array[String] ) {
		if( args.size < 1 ) {
			printUsage
		} else {
			//open a gearman
			val worker = GearmanWorker( args(0) )
			//register the "ftp_get" function to the gearman server
			worker.canDo ( "chat") {
				//if a job is fetched from the server 
				case ( content, uid, responser, dataFetcher ) =>
				    responser.data( "received:" + content )
				    //try to get data from the client side until "quit" message
				    //is received
				    var stop = false
				    while( !stop ) {
						dataFetcher.data match {
						    case "bye!" => 
								responser.complete("")
								stop = true
						    case data: String =>
								println( "received:" + data ) 
								responser.data( "received:" + data )
						}
					}
			}
		}
	}
	
	private def printUsage {
		println( "Usage:java -cp <classpath> org.gearman.example.ftp.FtpWorker server")
		println( "server\tis the address of gearman server in \"address:port\" format")
	}
}

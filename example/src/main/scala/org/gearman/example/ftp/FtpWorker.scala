package org.gearman.example.ftp

import org.gearman.worker._
import scala.io.Source

/**
 * This is the worker side of ftp implemented in gearman. It register "ftp_get" 
 * function to the gearman server. When a client submit a "ftp_get" job to the
 * gearman server, this worker will fetch the job from the gearman server and
 * open the file send the file contents to the client side line by line.   
 *
 * @author Steven Ou 
 */  
object FtpWorker {
	def main( args: Array[String] ) {
		if( args.size < 1 ) {
			printUsage
		} else {
			//open a gearman
			val worker = GearmanWorker( args(0) )
			//register the "ftp_get" function to the gearman server
			worker.canDo ( "ftp_get") {
				//if a job is fetched from the server 
				case ( fileName, uid, responser, _ ) =>
					//open and send the file content to client through gearman
					//server line by line
					Source.fromFile( fileName ).getLines.foreach {
						responser.data(_)
					}
					//ok, all the contents are sent, send a complete indication
					//to gearman server and client
					responser.complete("") 
			}
		}
	}
	
	private def printUsage {
		println( "Usage:java -cp <classpath> org.gearman.example.ftp.FtpWorker server")
		println( "server\tis the address of gearman server in \"address:port\" format")
	}
}
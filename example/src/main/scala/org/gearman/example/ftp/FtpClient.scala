package org.gearman.example.ftp

import org.gearman.client._


/**
 * This demo implements the "ftp" function in gearman.
 * 
 * The client submit a ftp_get request with file name and the worker
 * will send the whole file to the client side through the gearman server.
 * 
 * @author Steven Ou      
 */ 
 
object FtpClient {
	def main( args: Array[String] ) {
		if( args.size < 2 ) {
			printUsage
		} else {
			//connect to the gearman server
			val client = GearmanClient( args(0) )
			
			//submit "ftp_get" job with file server to
			//get the file contents and print it to screen
			client.submitJob( "ftp_get", args(1) ) {
				case JobData( data ) => println( data )
				case JobComplete( data ) =>			
			}
		}
	}
	
	private def printUsage {
		println( "Usage:java -cp <classpath> org.gearman.example.ftp.FtpWorker server fileName")
		println( "server\tis the address of gearman server in \"address:port\" format")
		println( "fileName\twhich file contents this client want to get")
	}
}
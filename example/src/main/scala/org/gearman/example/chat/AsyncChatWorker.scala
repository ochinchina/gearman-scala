package org.gearman.example.chat

import org.gearman.worker._
import scala.io.Source

/**
 * This is the worker side of "chat" implemented in gearman. It register "chat" 
 * function to the gearman server. When a client submit a "chat" job to the
 * gearman server, this worker will fetch the job from the gearman server and
 * exchange data between the client and the worker in asyn mode.   
 *
 * @author Steven Ou 
 */  
object AsyncChatWorker {
	def main( args: Array[String] ) {
		if( args.size < 1 ) {
			printUsage
		} else {
			//open a gearman
			val worker = GearmanWorker( args(0) )
			//register the "chat" function to the gearman server			
			worker.canDo ( "chat")(processChatData)
		}
	}
	
	private def processChatData(content:String,uid:Option[String],responser:JobResponser,dataFetcher:JobDataFetcher){
		content match {
			case "bye!" => 
				responser.complete("")
			case data:String =>
				println( "received:" + data ) 
				responser.data( "received:" + data )
				dataFetcher.data( processChatData )
		}
	}
	
	private def printUsage {
		println( "Usage:java -cp <classpath> org.gearman.example.ftp.FtpWorker server")
		println( "server\tis the address of gearman server in \"address:port\" format")
	}
}
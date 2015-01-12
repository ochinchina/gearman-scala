package org.gearman.example.reverse

import org.gearman.worker._

object ReverseWorker {
	def main( args: Array[ String ] ) {
		if( args.size < 1 ) {
			printUsage
		} else {
			//create a worker and connect to servers
			val worker = GearmanWorker( args(0) )
			//tell servers what work this worker can do 
			worker.canDo( "reverse") { case( data, uid, responser, _ ) =>
				//reverse the received data and response
				responser complete reverse( data )
			}
		}				
	}
	
	private def printUsage {
		println( "Usage: java -cp <classpath> org.gearman.example.reverse.ReverseWorker servers")
		println( "servers\tthe server address in \"server1:port1,server2:port2,...,servern:portn\" format")
	}
	private def reverse( data: String ) = new StringBuilder( data ).reverse.toString
}

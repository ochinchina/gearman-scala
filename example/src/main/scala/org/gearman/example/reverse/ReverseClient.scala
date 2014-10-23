package org.gearman.example.reverse

import org.gearman.client._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * The ReverseClient is a demo example that implements the client side of "reverse" 
 * function.
 *
 */   
object ReverseClient {
	def main( args: Array[ String ] ) {
		if( args.size < 2 ) {
			printUsage
		} else {
			//connect to the gearman server
			val client = GearmanClient( args(0) )
			for( i <- 1 until args.size ) {
				//submit a job
				client.submitJob( "reverse", args(i)) {
					//print the received result
					case JobComplete( data ) =>println( args(i) + "=>" + data )
				}
				.onSuccess {
					case jobHandle=>println( s"job handle:$jobHandle")
				}
			}
		}
	}
	
	private def printUsage {
		println( "Usage:java -cp <classpath> org.gearman.example.reverse.ReverseClient servers reverse1 reverse2 ... reversen")
		println( "servers\tthe server address in \"server1:port1,server2:port2,...,servern:portn\" format")
		println( "reversen\tthe reverse strings")
	}	
}
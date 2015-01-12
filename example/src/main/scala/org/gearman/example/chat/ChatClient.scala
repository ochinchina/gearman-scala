package org.gearman.example.chat

import org.gearman.client._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.LinkedList


/**
 * This demo implements the "chat" function in gearman.
 * 
 * The client submit a "chat" request to the worker at first, then exchange
 * the contents between the client & worker with WORK_DATA message. 
 * 
 * @author Steven Ou      
 */ 
 
object ChatClient {
	def main( args: Array[String] ) {
	
		if( args.size < 1 ) {
			printUsage
		} else {
			//connect to the gearman server
			val client = GearmanClient( args(0) )
			
			var jobDataSender: JobDataSender = null
			val talkingContents = new LinkedList[String]
			talkingContents.add( "Hello!")
			talkingContents.add( "How are you?" )
			talkingContents.add( "What are you doing recently?" )
			talkingContents.add( "bye!")
			
			//submit "chat" job with file server to
			//get the file contents and print it to screen
			client.submitJob( "chat", "") {
				case JobData( data ) => 
				    println( data )
				    jobDataSender.data( talkingContents.removeFirst )
				case JobComplete( data ) =>
					println( "job completed")			
			}.onSuccess {
			    case (jobHandle, tmpJobDataSender ) => jobDataSender = tmpJobDataSender
			}
		}
	}
		
	private def printUsage {
		println( "Usage:java -cp <classpath> org.gearman.examplechat.ChatClient server")
		println( "server\tis the address of gearman server in \"address:port\" format")
	}
}
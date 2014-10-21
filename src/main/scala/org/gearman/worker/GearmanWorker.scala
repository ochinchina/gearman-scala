/*
* Copyright (c) 2014 by its authors. Some rights reserved.
* See the project homepage at
*
* https://github.com/ochinchina/gearman-scala
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.gearman.worker

import org.gearman.message._
import org.gearman.channel._
import org.gearman.util.Util._
import scala.collection.mutable.{HashMap}
import scala.util.control.Breaks._
import java.net.InetSocketAddress
import java.util.concurrent.{Executors}
import scala.concurrent._
import ExecutionContext.Implicits.global

/**
 * responser for a job got from gearman server
 * 
 * @author Steven Ou  
 */ 
trait JobResponser {
	/**
	 * send data back to client
	 *  
	 * @param data the data to be sent	 	 
	 */	 	
	def data( data: String )
	
	/**
	 * send the status of the work to the client
	 * 
	 * @param numerator the percentage of numerator about the job
	 * @param denominator the percentage of denominator about the job 	 	 	 
	 */	 	
	def status( numerator: Int, denominator: Int )
	
	/**
	 * send a warning data to client
	 * 
	 * @param data the warning data	 	 
	 */	 		 
	def warning( data: String )
	
	/**
	 * indicate the job is completed
	 * 
	 * @param data the data to be sent to client when the job completed	 	 
	 */	 	
	def complete( data: String )
	
	/**
	 *  indicate the job is failed
	 */	 	
	def fail
	
	/**
	 *  indicate exception occurs when processing the job
	 *  
	 * @data the exception data	 	 
	 */	 	
	def exception( data: String )
}

/**
 * define interface to process the job got from gearman server
 *  
 * @author Steven Ou
 */  
trait JobHandler {
	/**
	 *  handle the received job
	 *  
	 * @param data the job data
	 * @param uid the job unique id setting by client side
	 * @param responser the job response interface, the job handler must send
	 * any feed back to client through this interface	  	 	 	 	 	 	 
	 */	 	
	def handle( data: String, 
				uid: Option[String],
				responser: JobResponser )
}

/**
 * this class manages all the on-going jobs fetched from the gearman server side
 * 
 * @author Steven Ou  
 */ 
class JobList {
	val jobs = new HashMap[ String, MessageChannel ]
	val channelJobs = new HashMap[ MessageChannel, java.util.LinkedList[ String ] ]
	
	/**
	 *  add a job to the list
	 *  
	 * @param jobHandle the job handle
	 * @param channel the message channel of gearman server	 	 	 
	 */	 	
	def addJob( jobHandle: String, channel: MessageChannel ) {
		jobs += jobHandle -> channel
		if( !channelJobs.contains( channel ) ) channelJobs += channel -> new java.util.LinkedList[ String ]
		channelJobs( channel ).add( jobHandle )
	}
	
	/**
	 * remove a job by the job handle
	 * 
	 * @param jobHandle the job handle	 	 
	 */	 	
	def removeJob( jobHandle: String ) {
		val channel = jobs.get( jobHandle ) 
		jobs -= jobHandle
		if( channel.nonEmpty && channelJobs.contains( channel.get ) ) {
			channelJobs( channel.get ).remove( jobHandle )
			if( channelJobs( channel.get ).isEmpty ) channelJobs -= channel.get
		}
	}
	
	/**
	 * get the number of total jobs
	 *  
	 * @return the number of total jobs	 	 
	 */	 	
	def size = jobs.size

	/**
	 *  get the number of jobs on the gearman server channel
	 *  
	 * @param channel the gearman server channel	 	 
	 */	 		
	def size( channel: MessageChannel ) = {
		if( channelJobs.contains( channel ) ) channelJobs( channel ).size else 0
	} 

}

/**
 * default job responser implementation
 * 
 * @param jobHandle the job handle to indicate which job this reponse is for
 * @param channel the gearman server channel
 * @param jobCompleted the callback for job completion. If the job is completed,
 * the jobCompleted callback will be called
 * 
 * @author Steven Ou       
 */ 
class DefJobResponser( jobHandle: String, channel: MessageChannel, jobCompleted: =>Unit ) extends JobResponser {

	override def data( data: String ) {
		channel.send( WorkDataReq( jobHandle, data ) )
	}
	
	override def status( numerator: Int, denominator: Int ) {
		channel.send( WorkStatusReq( jobHandle, numerator, denominator ) )
	}
	
	override def complete( data: String ) {
		jobCompleted
		channel.send( WorkCompleteReq( jobHandle, data ) )
	}
	
	override def warning( data: String ) {
		channel.send( WorkWarningReq( jobHandle, data ) )
	}
	
	override def fail {
		jobCompleted
		channel.send( WorkFailReq( jobHandle ) )
	}
	
	override def exception( data: String ) {
		jobCompleted
		channel.send( WorkExceptionReq( jobHandle, data ) )
	}
}

/**
 * create a Worker with gearman server address and the max number of on-going jobs
 * can be handled by the worker  
 *
 * @param servers the server address in "server1:port1,server2:port2,...,servern:portn"
 * format. If multiple gearman servers are provided, the worker will try to connect to 
 * all the gearman servers and get the job from them
 *
 * @param maxOnGoingJobs max number of jobs can be processed concurrently by worker
 * 
 * @author Steven Ou         
 */  
class GearmanWorker( servers: String, var maxOnGoingJobs: Int = 10 ) {
	val funcHandlers = new HashMap[ String, (JobHandler, Int ) ]
	val serverAddrs = parseAddressList( servers )
	val channels = new java.util.LinkedList[MessageChannel]
	// all the on-going jobs
	val jobs = new JobList
	val executor = Executors.newFixedThreadPool( 1 )

	@volatile	
	var stopped = false
	
	/**
	 * register the function to the gearman server
	 *
	 * @param funcName the function can be executed by this worker
	 * @param timeout the job with function name {@code funcName} can be finished
	 * within {@code timeout} seconds
	 * 
	 * @param funcHandle the function handler	 	 	 	 	 
	 */	 	 	
	def canDo( funcName: String, timeout: Int = -1 )( funcHandle: (String, Option[String], JobResponser) => Unit ) {
		canDo( funcName, new JobHandler {
			def handle( data: String, uid: Option[String], responser: JobResponser ) {
				funcHandle( data, uid, responser )
			}
		}, timeout )
	}
	
	/**
	 *  unregister a function handler by function name 
	 *  
	 * @param funcName the function name
	 */			
	def cantDo( funcName: String ) {
		executor.submit( new Runnable {
			def run {
				funcHandlers -= funcName
				broadcast( CantDo( funcName ) )
			}
		})
	}
	/**
	 *  register a function handler. Function handler can be registered
	 *  at any time	 
	 *  
	 * @param funcName the function name
	 * @param handler the handler to process the function
	 * @param timeout in seconds. If the timeout is greater than 0, the job should
	 * be finished within {@code timeout} seconds, if the worker can't finish it 
	 * within the timeout, the gearman server will fail the job. If the {@code timeout} 
	 * is less than or equal to 0, no timeout limit for the job	  	 
	 */	 		
	private def canDo( funcName: String, handler: JobHandler, timeout: Int ) {
		executor.submit( new Runnable {
			def run {
				funcHandlers += funcName -> ( handler, timeout )
				if( timeout > 0 ) broadcast( CanDoTimeout( funcName, timeout ) ) else broadcast( CanDo( funcName ) )
			}
		})
	}
	
	/**
	 *  start the gearman worker
	 */	 	
	def start() {
		stopped = false		
		for( i <- 0 until serverAddrs.size ) start( serverAddrs( i ) )			
	}
	
	/**
	 *  shutdown the worker
	 *  
	 * @param graceful do graceful shutdown	 	 
	 */	 	
	def shutdown( graceful: Boolean ) {
		stopped = true
		executor.submit( new Runnable {
			def run {
				if( graceful ) {
				} else {
					val iter = channels.iterator
					while( iter.hasNext ) try { iter.next.close } catch { case e: Throwable => }
					channels.clear 
				} 
			}
		})
	}
	
	private def start( addr: InetSocketAddress ) {		
		AsyncSockMessageChannel.asyncConnect( addr, channel => {
			if( channel == null ) {
				start( addr )
			} else {
				executor.submit( new Runnable {
					def run {
						channels.add( channel ) 
						channel.setMessageHandler( createMessageHandler( addr ) )
						funcHandlers.foreach{ case ( funcName, ( handler, timeout ) ) => 
							if( timeout > 0 ) 
								channel.send( CanDoTimeout( funcName, timeout ) ) 
							else channel.send( CanDo( funcName ) )
						}						 										
						channel.open
						grabJob( channel )						
					}
				} )
			}
		}, Some( executor ) )
	}
	
	private def createMessageHandler( addr: InetSocketAddress ) = new MessageHandler {
		override def handleMessage( msg: Message, from: MessageChannel ) {
			msg match {
				case JobAssign( jobHandle, funcName, data ) => handleJob( from, jobHandle, funcName, data, None )
				case JobAssignUniq( jobHandle, funcName, uid, data ) => handleJob( from, jobHandle, funcName, data, Some( uid ) )
				case Noop() => grabJob( from )
				case NoJob() => from.send( PreSleep() )																		
				case _ => from.send( PreSleep() )
			} 
		}
		
		override def handleDisconnect( from: MessageChannel ) {
			channels.remove( from )
			start( addr )
		}
	}
	
	private def handleJob( from: MessageChannel, jobHandle: String, funcName: String, data: String, uid: Option[String] ) {
		funcHandlers.get( funcName ) match {
			case Some( (handler, timeout) ) => 
				jobs.addJob( jobHandle, from )
				future { handler.handle( data, uid, new DefJobResponser( jobHandle, from, handleJobCompleted( from, jobHandle ) ) ) }
			case _ => from.send( Error( "2", "No handler found") )
		}
		
		grabJob( from )
	}
	
	private def handleJobCompleted( channel: MessageChannel, jobHandle: String ) {
		jobs.removeJob( jobHandle )
		if( stopped ) {
			if( jobs.size( channel ) <= 0 ) 
				try { channel.close } catch{ case e: Throwable => }
		} else grabJob( channel )
	}
	
	private def grabJob( channel: MessageChannel ) {
		if( maxOnGoingJobs <= 0 || jobs.size < maxOnGoingJobs ) channel.send( GrabJob() ) else channel.send( PreSleep() )
	}
	
	private def broadcast( msg: Message ) {		
		val iter = channels.iterator
		while( iter.hasNext ) {
			iter.next.send( msg )
		}
		
	} 
}

object GearmanWorker {
	
	def apply( servers: String, maxOnGoingJobs: Int = 10 ) = new GearmanWorker( servers, maxOnGoingJobs )
}

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
import java.util.concurrent.{Executors,LinkedBlockingQueue}
import scala.concurrent._
import scala.concurrent.duration.Duration
import ExecutionContext.Implicits.global

/**
 * responser for a job got from gearman server
 *
 * When a job is fetched from the server side, the worker proccess the job and
 * send the job status to the client through the [[JobResponser]] interface
 * 
 * When the job finished, the worker must call one of following methods:
 * 
 * - [[complete]]
 * <p> 
 * - [[fail]]
 * <p> 
 * - [[exception]]    
 *       
 * @see [[GearmanWorker]] 
 * @author Steven Ou  
 */ 
trait JobResponser {
	/**
	 * The worker sends updates, partial results or flushes data during long
	 * running job.	 
	 *
	 * The client will get a [[org.gearman.client.JobData]] event	  
	 *  
	 * @param data the data sent to client	 
	 */	 	
	def data( data: String )
	
	/**
	 * send the status of the work to the client
	 * 
	 * The client will get a [[org.gearman.client.JobStatus]] event	 	 
	 * 
	 * @param numerator the percentage of numerator about the job
	 * @param denominator the percentage of denominator about the job 	 	 	 
	 */	 	
	def status( numerator: Int, denominator: Int )
	
	/**
	 * send a warning data to client
	 * 
	 * The client will get a [[org.gearman.client.JobWarning]] event	 	 
	 * 
	 * @param data the warning data	 	 
	 */	 		 
	def warning( data: String )
	
	/**
	 * indicate the job is completed
	 * 
	 * The client will get a [[org.gearman.client.JobComplete]] event	 	 
	 * 
	 * @param data the data to be sent to client when the job completed	 	 
	 */	 	
	def complete( data: String )
	
	/**
	 *  indicate the job is failed
	 *  
	 * The client will get a [[org.gearman.client.JobFail]] event	 	 
	 */	 	
	def fail
	
	/**
	 *  indicate exception occurs when processing the job
	 *  
	 * The client will get a [[org.gearman.client.JobException]] event	 	 
	 *  
	 * @param data the exception data	 	 
	 */	 	
	def exception( data: String )
}

/**
 * define interface to process the job got from gearman server
 *  
 * @author Steven Ou
 */  
private trait JobHandler {
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
				responser: JobResponser,
				dataFetcher: JobDataFetcher )
}

trait JobDataFetcher {
    /**
     *  fetch the data from the client side in sync mode.
     *  
     * @return the data from the client side	      
     */	     
    def data: String
    
    /**
     *  fetch the data from the client side in async mode
     *  
     * @param callback the callback to receive the data from client side	      
     */	     
    def data( callback: (String, Option[String], JobResponser, JobDataFetcher)=>Unit )
} 

private class DefJobDataFetcher( uid: Option[String], responser: JobResponser ) extends JobDataFetcher {
    private val datas = new LinkedBlockingQueue[String]
        
    def add( data: String ) {
        datas.offer( data )
	}
    
	def data: String = datas.take
	
	def data( callback: (String, Option[String], JobResponser, JobDataFetcher)=>Unit ) {
		val dataFetcher = this
	    Future{ callback( datas.take, uid, responser, dataFetcher ) }
	}
}
/**
 * this class manages all the on-going jobs fetched from the gearman server side
 * 
 * @author Steven Ou  
 */ 
private class JobList {
	private val jobs = new HashMap[ String, (MessageChannel, DefJobDataFetcher) ]
	private val channelJobs = new HashMap[ MessageChannel, java.util.LinkedList[ String ] ]
	
	/**
	 *  add a job to the list
	 *  
	 * @param jobHandle the job handle
	 * @param channel the message channel of gearman server	 	 	 
	 */	 	
	def addJob( jobHandle: String, channel: MessageChannel, dataFetcher: DefJobDataFetcher ) {
		jobs += jobHandle -> ( channel, dataFetcher )
		if( !channelJobs.contains( channel ) ) channelJobs += channel -> new java.util.LinkedList[ String ]
		channelJobs( channel ).add( jobHandle )
	}
	
	/**
	 * remove a job by the job handle
	 * 
	 * @param jobHandle the job handle	 	 
	 */	 	
	def removeJob( jobHandle: String ) {
		jobs.get( jobHandle ) match {
		    case Some( (channel, dataFetcher) )=>
		        jobs -= jobHandle
		        if( channelJobs.contains( channel ) ) {
		            channelJobs( channel ).remove( jobHandle )
		            if( channelJobs( channel ).isEmpty ) channelJobs -= channel
				}
		    case _=>
		}
	}
	
	/**
	 * get the number of total jobs
	 *  
	 * @return the number of total jobs	 	 
	 */	 	
	def size = jobs.size

	/**
	 *  get the number of jobs on the channel
	 *  
	 * @param channel the gearman server channel	 	 
	 */	 		
	def size( channel: MessageChannel ) = {
		if( channelJobs.contains( channel ) ) channelJobs( channel ).size else 0
	}
	
	/**
	 *  get the job data fetcher by job handle
	 */	 	
	def getJobDataFetcher( jobHandle: String ):Option[DefJobDataFetcher] = {
	    var dataFetcher:DefJobDataFetcher = null
	    jobs( jobHandle ) match {
	        case (channel, tmpDataFetcher ) => dataFetcher = tmpDataFetcher
	        case _ =>
		}
		if( dataFetcher == null ) None else Some( dataFetcher )
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
private class DefJobResponser( jobHandle: String, channel: MessageChannel, jobCompleted: =>Unit ) extends JobResponser {

	private var completed = false
	
	override def data( data: String ) {
		if( !completed ) channel.send( WorkDataReq( jobHandle, data ) )
	}
	
	override def status( numerator: Int, denominator: Int ) {
		if( !completed ) channel.send( WorkStatusReq( jobHandle, numerator, denominator ) )
	}
	
	override def complete( data: String ) {
		if( !completed ) {
			completed = true
			jobCompleted
			channel.send( WorkCompleteReq( jobHandle, data ) )
		}
	}
	
	override def warning( data: String ) {
		if( !completed ) channel.send( WorkWarningReq( jobHandle, data ) )
	}
	
	override def fail {
		if( !completed ) {
			completed = true
			jobCompleted
			channel.send( WorkFailReq( jobHandle ) )
		}
	}
	
	override def exception( data: String ) {
		if( !completed ) {
			completed = true
			jobCompleted
			channel.send( WorkExceptionReq( jobHandle, data ) )
		}
	}
}

/**
 * Create a Worker with gearman server address and the max number of on-going jobs
 * can be handled by the worker
 * 
 * The created worker will try to connect with all the servers listed in the {@code servers}
 * parameter. If a server listed in the {@code servers} parameter can not be connected,
 * the gearman server will try to connect it repeatedly until the connection is
 * established.  
 *                                 
 * User should call [[canDo]] method with the function handler to tell the gearman
 * server what type of the job this worker can do. The [[canDo]] method can be called
 * at any time.
 * 
 * The following piece of code demostrates how the worker works:
 * {{{
 * //connect to servers: 192.168.1.1 with port 4730 and 192.168.1.2 with port 4730
 * val worker = GearmanWorker( "192.168.1.1:4730,192.168.1.2:4730")
 * //tells the server this worker can do string reverse with a worker hander
 * //work handler accepts three parameters:
 * //-data: the data in the SUBMIT_JOB_XXX by client 
 * //-uid: the unique identifier assigned by client in SUBMIT_JOB_XXX
 * //-responser: instance of JobResponser     
 * worker.canDo( "reverse") { case (data, uid, responser) =>
 * 	//reverse the received string data & send result to the client 
 *		responser complete reverse( data )    
 *  }             
 *  
 *	private def reverse( s: String ): String = {
 *		//implement your reverse algorithm 
 *	}    
 * }}}              
 * 
 * @param servers the server address in "server1:port1,server2:port2,...,servern:portn"
 * format. If multiple gearman servers are provided, the worker will try to connect to 
 * all the gearman servers and get the job from them
 *
 * @param maxOnGoingJobs max number of jobs can be processed concurrently by worker
 * 
 * @see [[JobResponser]] 
 * @author Steven Ou         
 */  
class GearmanWorker( servers: String, var maxOnGoingJobs: Int = 10 ) {
	private val funcHandlers = new HashMap[ String, (JobHandler, Int ) ]
	private val serverAddrs = parseAddressList( servers )
	private val channels = new java.util.LinkedList[MessageChannel]
	// all the on-going jobs
	private val jobs = new JobList
	private val executor = Executors.newFixedThreadPool( 1 )

	@volatile	
	private var stopped = false

	//start the worker	
	start
	
	/**
	 * tells the gearman server what work the worker can do with the function
	 * handler that handles jobs fetched from server	 
	 * 
	 *
	 * @param funcName what function can be done by this worker
	 * @param timeout > 0 the job with function name {@code funcName} can be finished
	 * within {@code timeout} seconds, <= 0, no timeout for the function
	 * 
	 * @param funcHandle the function handler with three parameters. First parameter is the
	 * function data, the second parameter is the optional job uid and the third
	 * parameter is the job responser used to send data to the client	 	 	 	 	 	 	 
	 */	 	 	
	def canDo( funcName: String, timeout: Int = -1 )( funcHandle: (String, Option[String], JobResponser, JobDataFetcher ) => Unit ) {
		canDo( funcName, new JobHandler {
			def handle( data: String, uid: Option[String], responser: JobResponser, dataFetcher: JobDataFetcher ) {
				funcHandle( data, uid, responser, dataFetcher )
			}
		}, timeout )
	}
	
	/**
	 *  tells the gearman server this worker will not do the previous registered
	 *  function through [[canDo]] method	 
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
	 *  shutdown the worker in block mode
	 *  
	 * @param graceful true shutdown the worker in graceful way: 1) will not fetch
	 * any jobs from server 2) waiting for all on-going jobs completed	 
	 * <p>	                                 
	 * false shutdown the worker immediatelly even if there are on-going job	  	 	 
	 */	 	
	def shutdown( graceful: Boolean ) {
		stopped = true
		val p = Promise[Boolean]()
		executor.submit( new Runnable {
			def run {
				if( graceful ) {
					if( channels.size <= 0 ) p.success(true) else executor.submit( this ) 
				} else {
					val iter = channels.iterator
					while( iter.hasNext ) try { iter.next.close } catch { case e: Throwable => }
					channels.clear
					p.success(true) 
				} 
			}
		})
		
		Await.ready( p.future, Duration.Inf )
	}
	
	/**
	 *  start the gearman worker
	 */	 	
	private def start() {
		stopped = false		
		for( i <- 0 until serverAddrs.size ) start( serverAddrs( i ) )			
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
		} )
	}
	
	private def createMessageHandler( addr: InetSocketAddress ) = new MessageHandler {
		override def handleMessage( msg: Message, from: MessageChannel ) {
			msg match {
				case JobAssign( jobHandle, funcName, data ) => handleJob( from, jobHandle, funcName, data, None )
				case JobAssignUniq( jobHandle, funcName, uid, data ) => handleJob( from, jobHandle, funcName, data, Some( uid ) )
				case Noop() => grabJob( from )
				case NoJob() => from.send( PreSleep() )
				case WorkDataReq( jobHandle, data ) => handleJobData( from, jobHandle, data )																		
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
				val responser = new DefJobResponser( jobHandle, from, handleJobCompleted( from, jobHandle ) )
			    val dataFetcher = new DefJobDataFetcher( uid, responser ) 
				jobs.addJob( jobHandle, from, dataFetcher )
				Future{ handler.handle( data, uid, responser, dataFetcher ) }
			case _ =>
		}
		
		grabJob( from )
	}
	
	private def handleJobData( from: MessageChannel, jobHandle: String, data: String ) {
	    jobs.getJobDataFetcher( jobHandle ) match {
	        case Some( dataFetcher ) => dataFetcher.add( data )
	        case _ =>
		}
	}
	
	private def handleJobCompleted( channel: MessageChannel, jobHandle: String ) {
		jobs.removeJob( jobHandle )
		if( stopped ) {
			if( jobs.size( channel ) <= 0 ) 
				try { channel.close } catch{ case e: Throwable => }
		} else grabJob( channel )
	}
	
	private def grabJob( channel: MessageChannel ) {
		if( !stopped && (maxOnGoingJobs <= 0 || jobs.size < maxOnGoingJobs ) ) channel.send( GrabJobUniq() ) else channel.send( PreSleep() )
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

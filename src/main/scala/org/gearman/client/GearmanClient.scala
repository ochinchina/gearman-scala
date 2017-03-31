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
package org.gearman.client

import org.gearman.message._
import org.gearman.channel._
import org.gearman.util.Util._
import java.util.{ LinkedList, Timer, TimerTask, UUID }
import scala.util.control.Breaks._
import java.nio.channels.{AsynchronousSocketChannel,CompletionHandler }
import java.util.concurrent.{Executors, ExecutorService}
import java.net.{InetSocketAddress}
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global


/**
 * After submitting a job to the server, who will schedule the job to the registered
 * worker, the worker will send the status of the job to the sender. The client
 * will convert the gearman message to [[JobEvent]] and dispatch it to the job callback
 * in the method [[GearmanClient.submitJob]].  
 * 
 * This interface is just used to identify a JobEvent without any data members and
 * methods.
 * 
 * @see [[GearmanClient.submitJob]]       
 * 
 * @author Steven Ou  
 */
trait JobEvent

/**
 * After created a job in the gearman, the client can send a lot of data to the
 * worker by invoking the method    
 */ 
trait JobDataSender {
    def data(data: String )
}

/**
 * Client will convert the WORK_DATA message to a JobData event
 * 
 * This event will dispatch to the callback of [[GearmanClient.submitJob]] method
 * 
 * @param data the data part in the WORK_DATA message  
 *
 */  
case class JobData( data: String ) extends JobEvent
/**
 * Client will convert the WORK_WARNING message to a JobWarning event
 * 
 * This event will dispatch to the callback of [[GearmanClient.submitJob]] method
 * 
 * @param data the data part in the WORK_WARNING message  
 *
 */  
case class JobWarning( data: String ) extends JobEvent

/**
 * This event will be used in following two situations:
 *
 * <p>
 * 1, Client submit a job, the worker sends WORK_STATUS to the client to update
 * the job status. At this time, both the {@code knownStatus } and the {@code runningStatus}
 * are true.
 * 
 * <p>
 * 2, Client query the status of submitted job by invoking [[GearmanClient.getStatus]] method, the
 * server will send the GET_STATUS_RES to the client. The GET_STATUS_RES message will
 * also be converted to [[JobStatus]] event. At this time, the {@code knownStatus} and
 * {@code runningStatus} reflects the status of the job              
 * 
 * This event will dispatch to the callback of [[GearmanClient.submitJob]] method
 * 
 * @param knownStatus true if the status of submitted job is known, false if the status of submitted job is unknown
 * @param runningStatus true the submitted job is in running state, false if the submitted job is still in pending state
 * @param numerator the complete percentage numerator
 * @param denominator the complete percentage denominator      
 *
 */  
case class JobStatus( knownStatus:Boolean, runningStatus: Boolean, numerator: Int, denominator: Int  ) extends JobEvent

/**
 * Client will convert the WORK_COMPLETE message to a JobComplete event
 * 
 * This event will dispatch to the callback of [[GearmanClient.submitJob]] method
 * 
 * @param data the data part in the WORK_COMPLETE message  
 *
 */  
case class JobComplete( data: String  ) extends JobEvent

/**
 * Client will convert the WORK_FAIL message to a JobFail event
 * 
 * This event will dispatch to the callback of [[GearmanClient.submitJob]] method
 * 
 * @param data the data part in the WORK_FAIL message  
 *
 */ 
case class JobFail() extends JobEvent

/**
 * Client will convert the WORK_EXCEPTION message to a JobFail event
 * 
 * This event will dispatch to the callback of [[GearmanClient.submitJob]] method
 * 
 * @param data the data part in the WORK_EXCEPTION message  
 *
 */ 
case class JobException( data: String ) extends JobEvent

/**
 * if the connection to gearman server is lost, the on-going jobs will get
 * connection lost event
 */ 
case class JobConnectionLost() extends JobEvent

/**
 * if the timeout parameter is set in the [[GearmanClient.submitJob]], a timer
 * will be started to monitor if the job can be finished before timeout. If the
 * job is not finished( not get JobComplete, JobException or JobFail event), the
 * job will receive a [[JobTimeout]] event in its callback    
 *
 */  
case class JobTimeout() extends JobEvent

private class DefMessageChannelFactory(servers: String ) extends MessageChannelFactory {
	private val serverAddrs = parseAddressList( servers )
	
	def create( executor: ExecutorService, callback: MessageChannel => Unit ) {
		start( 0, executor, callback )
	}
	
	private def start( index: Int, executor: ExecutorService, callback: MessageChannel => Unit ) {
		if( index >= serverAddrs.size ) {
			start( 0, executor, callback )
		} else {
			AsyncSockMessageChannel.asyncConnect( serverAddrs( index ), 
							{ channel: MessageChannel=> if( channel != null ) callback( channel ) else start( index + 1, executor, callback ) } )
		}
	}
}  

/**
 *  represents the client side in gearman protocol. When a user can submit a job
 *  to the server and server will schedule the job to a worker. The worker will
 *  report the job status to the client.
 *  
 *  ==submit job to server==
 *  User can submit a job to server by calling [[submitJob]] method. The following
 *  code demostrates how a user can submit job to server
 *  
 * {{{
 * //connect to one of following servers:
 * // 192.168.1.1 with port number 4730
 * // 192.168.1.2 with port number 4730
 * //If the connection to server is broken, client will connect to the server in
 * //the background automatically      
 * val client = GearmanClient( "192.168.1.1:4730,192.168.1.2:4730")
 *
 * //submit a job with "reverse" function name, data "hello, world!" and a job
 * //callback which accepts the JobEvent    
 * client.submitJob( "reverse", "hello, world!") {
 *  //print the reverse string of "hello,world" 
 * 	case JobComplete( result ) => println( result )
 * 	case JobConnectionLost() => println( "connect lost to the server")  
 * }   
 * }}}   
 *    
 *  ==submit a background job to server==   
 *  
 *  A background job can be submitted to the server if the client don't want
 *  to get the status of the submitted job. Following code demostrates how to
 *  submit a background job from client side:
 *  
 * {{{
 * //connect to one of following servers:
 * // 192.168.1.1 with port number 4730
 * // 192.168.1.2 with port number 4730
 * //If the connection to server is broken, client will connect to the server in
 * //the background automatically      
 * val client = GearmanClient( "192.168.1.1:4730,192.168.1.2:4730")
 * 
 * //submit a background job with function name "WriteToFile" and data "hello, world!"
 * client.submitJobBg( "WriteToFile", "hello, world!")   
 * }}}        
 *                    
 * @param servers the gearman server address list, the address list is in
 * "server1:port,server2:port,...,servern:port"
 * @param maxOnGoingJobs > 0 the max number of jobs can be submitted to gearman server,
 * <=0 no limitation on the jobs submitted to gearman server at same time
 * <p>
 * if the number of submitted job reaches the {@code maxOnGoingJobs} limitation, the
 * submitJob method will put the job to local queue until a running job is finished.     
 * 
 */ 
class GearmanClient( channelFactory: MessageChannelFactory, maxOnGoingJobs: Int = 10 ) {
	import org.gearman.message.JobPriority._
	import Array._
	
	private var stopped = false
	
	private var clientChannel: MessageChannel = null	
	private val runningJobs = new LinkedList[JobInfo]
	private val pendingJobs = new LinkedList[JobInfo]
	private val executor = Executors.newFixedThreadPool( 1 )
	private val timer = new Timer
	
	channelFactory.create( executor, channelCreated )

	private case class JobInfo( msg: Message, timeout: Long, respChecker: ResponseChecker )
	private case class ResponseCheckResult( isMyResponse: Boolean, finished: Boolean )
	
	private trait ResponseChecker {
		def checkResponse( msg: Option[Message], connLost: Boolean, timeout: Boolean ): ResponseCheckResult
	}
	
	/**
	 *  ping gearman server by sending ECHO message 
	 *  
	 * @param data the data sent to the server
	 * @param timeout > 0 timeout in milliseconds, <= 0 no timeout
	 * 
	 * @return the echo data in Future	 	  	 	 	 
	 */	 	
	def echo( data: String, timeout: Int = -1 ):Future[String] = {
		val p = Promise[String]()
				
		send( EchoReq( data ), timeout, new ResponseChecker{
			override def checkResponse( msg: Option[Message], connLost: Boolean, timeout: Boolean ): ResponseCheckResult = {
				if( connLost ) {
					p failure ( new Exception("Communication Lost") )
					ResponseCheckResult( true, true )
				} else if( timeout ) {
					p failure ( new Exception("timeout") )
					ResponseCheckResult( true, true )
				} else msg match {
					case Some( EchoRes( resData ) ) => 
						p success resData
						ResponseCheckResult( true, true )
					case _ => ResponseCheckResult( false, false )
				}
			}
			
		}, false )
		
		p.future 
	}
		
	
	
    /**
	 * submit a normal priority job with {@code funcName}, {@code uid}, {@code data}, 
	 * , {@code timeout} and {@code callback} to server
	 *	 * 	  	  
	 * The callback will receive the data sent by the worker	 	 
	 * 
	 * @param funcName the function name
	 * @param data the data
	 * @param uid the unique identifier
	 * @param timeout > 0 timeout in milliseconds, <= 0 no timeout 	 
	 * @param callback to receive the data from worker
	 * 	 	 
	 * @return a Future[String] contains the returned job handler	 	 	 	 	  	 	 
	 */	 
	def submitJob( funcName: String, data: String, uid: String = UUID.randomUUID.toString, timeout: Long = -1 )( jobCallback: JobEvent=>Unit  ): Future[ (String,JobDataSender) ] = submitJob( funcName, data, uid, JobPriority.Normal, timeout, Some( jobCallback ) )
		
	/**
	 * submit a normal priority background job with {@code funcName}, {@code data}, 
	 * {@code uid}, and {@code timeout} to server
	 * 
	 * @param funcName the function name
	 * @param data the data
	 * @param uid the unique identifier
	 * @param timeout > 0 the timeout in milliseconds, <= 0 no timeout
	 * @return a Future[String] contains the returned job handler	 	 	 	 	  	 	 
	 */	 	
	def submitJobBg( funcName: String, data: String, uid: String = UUID.randomUUID.toString, timeout: Long = -1 ): Future[ (String,JobDataSender) ] = submitJob( funcName, data, uid, JobPriority.Normal, timeout, None )

    /**
	 * submit a low priority job with {@code funcName}, {@code uid}, {@code data}, 
	 * , {@code timeout} and {@code callback} to server
	 *	 * 	  	  
	 * The callback will receive the data sent by the worker	 	 
	 * 
	 * @param funcName the function name
	 * @param data the data
	 * @param uid the unique identifier
	 * @param timeout > 0 timeout in milliseconds, <= 0 no timeout 	 
	 * @param callback to receive the data from worker
	 * 	 	 
	 * @return a Future[String] contains the returned job handler	 	 	 	 	  	 	 
	 */	
	def submitJobLow( funcName: String, data: String, uid: String = UUID.randomUUID.toString, timeout: Long = -1 )( jobCallback: JobEvent=>Unit ): Future[ (String,JobDataSender) ] = submitJob( funcName, data, uid, JobPriority.Low, timeout, Some(jobCallback ) )
	
	/**
	 * submit a low priority background job with {@code funcName}, {@code data}, 
	 * {@code uid}, and {@code timeout} to server
	 * 
	 * @param funcName the function name
	 * @param data the data
	 * @param uid the unique identifier
	 * @param timeout > 0 the timeout in milliseconds, <= 0 no timeout
	 * @return a Future[String] contains the returned job handler	 	 	 	 	  	 	 
	 */	 	
	def submitJobLowBg( funcName: String, data: String, uid: String = UUID.randomUUID.toString, timeout: Long = -1 ): Future[ (String,JobDataSender) ] = submitJob( funcName, data, uid, JobPriority.Low, timeout, None)

    /**
	 * submit a high priority job with {@code funcName}, {@code uid}, {@code data}, 
	 * , {@code timeout} and {@code callback} to server
	 *	 * 	  	  
	 * The callback will receive the data sent by the worker	 	 
	 * 
	 * @param funcName the function name
	 * @param data the data
	 * @param uid the unique identifier
	 * @param timeout > 0 timeout in milliseconds, <= 0 no timeout 	 
	 * @param callback to receive the data from worker
	 * 	 	 
	 * @return a Future[String] contains the returned job handler	 	 	 	 	  	 	 
	 */
	def submitJobHigh( funcName: String, data: String, uid: String = UUID.randomUUID.toString, timeout: Long = -1 )( jobCallback: JobEvent=>Unit ): Future[ (String,JobDataSender) ] = submitJob( funcName, data, uid, JobPriority.High, timeout, Some(jobCallback ) )
	
	/**
	 * submit a high priority background job with {@code funcName}, {@code data}, 
	 * {@code uid}, and {@code timeout} to server
	 * 
	 * @param funcName the function name
	 * @param data the data
	 * @param uid the unique identifier
	 * @param timeout > 0 the timeout in milliseconds, <= 0 no timeout
	 * @return a Future[String] contains the returned job handler	 	 	 	 	  	 	 
	 */	 	
	def submitJobHighBg( funcName: String, data: String, uid: String = UUID.randomUUID.toString, timeout: Long = -1 ): Future[ (String,JobDataSender) ] = submitJob( funcName, data, uid, JobPriority.High, timeout, None)
	
	/**
	 *  get the job status
	 *  
	 * @param jobHandle the job handle returned from [[submitJob]] or [[submitJobBg]]
	 * @param timeout >0 timeout in millisconds, <=0 no timeout
	 *
	 * @see [[submitJob]] [[submitJobBg]]	  
	 * @return the job status	 	 
	 */
	def getStatus(  jobHandle: String, timeout: Long = -1 ) : Future[ JobStatus ] = {
		val p = Promise[ JobStatus]()
		send( GetStatus( jobHandle), timeout, new ResponseChecker {
			override def checkResponse( msg: Option[Message], connLost: Boolean, timeout: Boolean ): ResponseCheckResult = {
				if( connLost ) {
					p failure ( new Exception( "Communication Lost") )
					ResponseCheckResult( true, true )
				} else if( timeout ) {
					p failure (new Exception( "message timeout") )
					ResponseCheckResult( true, true )
				} else msg match {
					case Some( StatusRes( resJobHandle, knownStatus, runningStatus, percentCompleteNumerator, percentCompleteDenominator ) ) =>
						if( resJobHandle == jobHandle ) {
							p success JobStatus( knownStatus, runningStatus, percentCompleteNumerator, percentCompleteDenominator )
							ResponseCheckResult( true, true )
						} else ResponseCheckResult( false, false )
					case _ => ResponseCheckResult( false, false )
				}
			}}, false )
		
		p.future		
	}
	
	/**
	 *  shutdown the client gracefully.
	 *  
	 *  The on-going job will be finished and all the submitted jobs will fail
	 *  ( by checking the returned Future in the [[submitJob]]/[[submitJobBg]]	 	 	 
	 */	 	
	def shutdown {
		stopped = true
		val p = Promise[ Boolean ]()
		executor.submit( new Runnable {
			def run {
				if( pendingJobs.size > 0 || runningJobs.size > 0 ) {
					executor.submit( this )
				} else {
					try { clientChannel.close } catch { case ex:Throwable => }
					p success true
				} 
			}
		} )
		
		Await.ready( p.future, Duration.Inf )
	}

    /**
	 *  submit a job to the server and return a {@link Future[String]} to present
	 *  the job handle	  
	 *  
	 * @param funcName the function name
	 * @param data the function data
	 * @param uid unique identifier
	 * @param priority the job priority, must be Normal, High or Low, default is
	 * 	Normal	 
	 * @param timeout the optional timeout in milliseconds
	 * @param callback the optional callback used to receive the job status, 
	 * 	 error, exception, data and completion info. If no callback is provided,
	 * 	 the job will be submitted as background job	  
	 * 
	 * @return job handle {@link Future[String]} 	 	 	 	  	 	 	 	 	 
	 */	 	
	private def submitJob( funcName: String, 
				data: String, 
				uid: String, 
				priority:JobPriority, 
				timeout: Long, 
				callback: Option[JobEvent=>Unit] ): Future[(String,JobDataSender)] = {
				
		val p = Promise[(String,JobDataSender)]()
		send( createSubmitJobMessage( funcName, data, uid, callback.isEmpty, priority ), 
			timeout, 
			new JobResponseChecker( callback, p ),
			callback.isEmpty  )
		p.future 
	}
	
	private def createSubmitJobMessage( funcName: String, data: String, uid: String, background: Boolean, priority:JobPriority ) = {
		priority match {
			case JobPriority.Normal => if( background ) SubmitJobBg( funcName, uid, data ) else SubmitJob( funcName, uid, data )
			case JobPriority.High => if( background ) SubmitJobHighBg( funcName, uid, data ) else SubmitJobHigh( funcName, uid, data ) 
			case _ => if( background ) SubmitJobLowBg( funcName, uid, data ) else SubmitJobLow( funcName, uid, data )
		}
	}
	
	private def channelCreated( channel: MessageChannel ) {
		val msgHandler = new MessageHandler {
			override def handleMessage( msg: Message, from: MessageChannel ) {
				doResponseCheck( msg )
			}
			
			override def handleDisconnect( from: MessageChannel ) {
				clientChannel = null
				notifyConnectionLost
				if( !stopped ) channelFactory.create( executor, channelCreated )
			}
		}
		
		clientChannel = channel
		channel.setMessageHandler( msgHandler )
		channel.open
		sendPendingJobs
	}
		
	private def sendPendingJobs {
		if(  clientChannel != null && pendingJobs.size > 0 && ( maxOnGoingJobs <= 0 || runningJobs.size < maxOnGoingJobs ) ) {
			val jobInfo = pendingJobs.removeFirst					
			runningJobs.add( jobInfo )			

			clientChannel.send( jobInfo.msg, Some( success => {if( !success ) pendingJobs.add( jobInfo )} ) )
		}
	}
	
	
	private def send( msg: Message, timeout:Long, respChecker: ResponseChecker, background: Boolean  ) {
		if( stopped ) {		
			throw new Exception( "in shutdown, no message will be sent to server")
		}
		val jobInfo = JobInfo( msg, timeout, respChecker )
		executor.submit( new Runnable {
			def run {
				if( timeout > 0 && !background ) startJobTimeoutTimer( timeout, jobInfo )
				pendingJobs.add( jobInfo )
				sendPendingJobs
			}
		})		
	}
	
	private def startJobTimeoutTimer( timeout:Long, jobInfo: JobInfo ) {
		timer.schedule( new TimerTask {
			def run {
				executor.submit( new Runnable {
					def run {
						if( runningJobs.remove( jobInfo ) ) {
							jobInfo.respChecker.checkResponse( None, false, true )
						}
					}
				})
			} 
		}, timeout )
	}
	
	private def doResponseCheck(msg:Message) {
		breakable {
			val iter = runningJobs.iterator	
			while( iter.hasNext ) {
				val jobInfo = iter.next
				val checkResult = jobInfo.respChecker.checkResponse( Some( msg ), false, false )
				if( checkResult.isMyResponse ) {
					if( checkResult.finished ) runningJobs.remove( jobInfo )
					break
				}
			}
		}
	}
	
	private def notifyConnectionLost() {
		val iter = runningJobs.iterator
		
		while( iter.hasNext ) {
			iter.next.respChecker.checkResponse( None, true, false )
		}
		runningJobs.clear
	}
	
	private class JobResponseChecker( callback: Option[JobEvent=>Unit], p: Promise[ (String, JobDataSender) ] ) extends ResponseChecker {
		@volatile
		var thisJobHandle: String = null
				
		def checkResponse( msg: Option[Message], connLost: Boolean, timeout: Boolean ): ResponseCheckResult = {
			if( connLost ) {
				if( callback.nonEmpty ) callback.get( JobConnectionLost() )
				ResponseCheckResult( true, true )
			} else if( timeout ) {
             	if( callback.nonEmpty ) callback.get( JobTimeout() )
             	ResponseCheckResult( true, true )
            } else if( msg.isEmpty ) {
				ResponseCheckResult( false, false ) 
			} else processMessage( msg.get )
		}
			
		private def processMessage( msg: Message ) = {
			msg match {
					case JobCreated( jobHandle ) => handleJobCreated( jobHandle )
					case WorkDataRes( jobHandle, data ) => handleWorkDataRes( jobHandle, data )
					case WorkWarningRes( jobHandle, data ) => handleWorkWarningRes( jobHandle, data )
					case WorkStatusRes( jobHandle, numerator, denominator ) => handleWorkStatusRes( jobHandle, numerator, denominator )
					case WorkCompleteRes( jobHandle, data ) => handleWorkCompleteRes( jobHandle, data )
					case WorkFailRes( jobHandle ) => handleWorkFailRes( jobHandle )
					case WorkExceptionRes( jobHandle, data ) => handleWorkExceptionRes( jobHandle, data )
					case _ => ResponseCheckResult( false, false )					
			}
		}
			
		private def handleJobCreated( jobHandle: String ) = {
			if( thisJobHandle == null ) {
				thisJobHandle = jobHandle
				p success ( (jobHandle, createJobDataSender(jobHandle) ) )
				if( callback.isEmpty ) ResponseCheckResult( true, true ) else ResponseCheckResult( true, false )
			} else ResponseCheckResult( false, false )

		}
		
		private def handleWorkDataRes( jobHandle: String, data: String ) = {
			if( jobHandle == thisJobHandle ) {
				callback.get( JobData( data ) )
				ResponseCheckResult( true, false )
			} else ResponseCheckResult( false, false )
		}
		
		private def handleWorkWarningRes( jobHandle: String, data: String ) = {
			if( jobHandle == thisJobHandle ) {
				callback.get( JobWarning( data ) )
				ResponseCheckResult( true, false )
			} else ResponseCheckResult( false, false )			
		}
		
		private def handleWorkStatusRes( jobHandle: String, numerator:Int, denominator: Int ) = {
			if( jobHandle == thisJobHandle ) {
				callback.get( JobStatus( true, true, numerator, numerator ) )
				ResponseCheckResult( true, false )
			} else ResponseCheckResult( false, false )
		}
		
		private def handleWorkCompleteRes( jobHandle: String, data: String ) = {
		    if( jobHandle == thisJobHandle ) {
				callback.get( JobComplete( data ) )
				ResponseCheckResult( true, true )
			} else ResponseCheckResult( false, false )
		}
		
		private def handleWorkFailRes( jobHandle: String ) = {
			if( jobHandle == thisJobHandle ) {
				callback.get( JobFail() )
				ResponseCheckResult( true, true )
			} else ResponseCheckResult( false, false )
		}
		
		private def handleWorkExceptionRes( jobHandle: String, data: String ) = {
			if( jobHandle == thisJobHandle ) {
				callback.get( JobException( data ) )
				ResponseCheckResult( true, false )
			} else ResponseCheckResult( false, false )
		}
		
		private def createJobDataSender( jobHandle: String ):JobDataSender = new JobDataSender {
		    def data( data: String ) {
		        executor.submit( new Runnable{
		           def run {
				       try {
					       clientChannel.send( WorkDataReq( jobHandle, data ) )
                       }catch {
                           case ex:Throwable=>
					   }
				   }
				})
			}
		} 
	}
}

object GearmanClient {
	 /**
	  * @param servers the gearman server address list, the address list is in
	  * "server1:port,server2:port,...,servern:port"
	  * @param maxOnGoingJobs > 0 the max number of jobs can be submitted to gearman server,
	  * <=0 no limitation on the jobs submitted to gearman server at same time
	  */      

	def apply( servers: String, maxOnGoingJobs: Int = 10 ) = new GearmanClient( new DefMessageChannelFactory( servers ), maxOnGoingJobs )

	def apply( channelFactory: MessageChannelFactory ): GearmanClient = apply( channelFactory, 10 )
		
	def apply( channelFactory: MessageChannelFactory, maxOnGoingJobs: Int ) = new GearmanClient( channelFactory, maxOnGoingJobs ) 
	
	
}



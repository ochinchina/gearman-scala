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
import java.util.{ LinkedList, Timer, TimerTask }
import scala.util.control.Breaks._
import java.nio.channels.{AsynchronousSocketChannel,
						CompletionHandler }
import java.util.concurrent.{Executors}
import java.net.{InetSocketAddress}
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * the job callback. When gearman client submits a job to gearman server, a job
 * callback must be provided to receive job related data. 
 * 
 * @author Steven Ou  
 */
trait JobEvent {
}

case class JobData( data: String ) extends JobEvent
case class JobWarning( data: String ) extends JobEvent
case class JobStatus( knownStatus:Boolean, runningStatus: Boolean, numerator: Int, denominator: Int  ) extends JobEvent
case class JobComplete( data: String  ) extends JobEvent
case class JobFail() extends JobEvent
case class JobException( data: String ) extends JobEvent
case class JobConnectionLost() extends JobEvent
case class JobTimeout() extends JobEvent
 


trait JobCallback {
	def handle( event: JobEvent )
}

class JobCallbackProxy( callback: JobCallback ) extends JobCallback {
	override def handle( event: JobEvent ) {
		future { callback.handle( event) }
	}
}

/**
 *  construct a GearmanClient object
 *                    
 * @param servers the gearman server address list, the address list is in
 * "server1:port,server2:port,...,servern:port"
 * @param maxOnGoingJobs the max number of jobs can be sent to gearman server
 * 
 */ 
class GearmanClient( servers: String, maxOnGoingJobs: Int = 10 ) {
	import org.gearman.message.JobPriority._
	import Array._
	
	private val serverAddrs = parseServers
	private var stopped = false
	
	private var clientChannel: MessageChannel = null	
	private val runningJobs = new LinkedList[JobInfo]
	private val pendingJobs = new LinkedList[JobInfo]
	private val executor = Executors.newFixedThreadPool( 1 )
	private val timer = new Timer
	private val BackGroundJobCallback = { event: JobEvent => }
	
	start( 0 ) 

	case class JobInfo( msg: Message, timeout: Int, respChecker: ResponseChecker )
	case class ResponseCheckResult( isMyResponse: Boolean, finished: Boolean )
	
	trait ResponseChecker {
		def checkResponse( msg: Option[Message], connLost: Boolean, timeout: Boolean ): ResponseCheckResult
	}
	
	/**
	 *  execute ECHO message
	 *  
	 * @param data the data sent to the server
	 * @param timeout > 0 timeout in seconds, <= 0 no timeout
	 * 
	 * @return the echo data in Future	 	  	 	 	 
	 */	 	
	def echo( data: String, timeout: Int = -1 ):Future[String] = {
		val p = Promise[String]
		
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
			
		})
		
		p.future 
	}
		
	
	/**
	 *  submit a job to the server and return a {@link Future[String]} to present
	 *  the job handle	  
	 *  
	 * @param funcName the function name
	 * @param uid unique identifier
	 * @param data the function data
	 * @param priority the job priority, must be Normal, High or Low, default is
	 * 	Normal	 
	 * @param timeout the optional timeout in seconds
	 * @param callback the optional callback used to receive the job status, 
	 * 	 error, exception, data and completion info. If no callback is provided,
	 * 	 the job will be submitted as background job	  
	 * 
	 * @return job handle {@link Future[String]} 	 	 	 	  	 	 	 	 	 
	 */	 	
	def submitJob( funcName: String, 
				uid: String, 
				data: String, 
				priority:JobPriority, 
				timeout: Int, 
				callback: Option[JobEvent=>Unit] ): Future[String] = {
				
		val p = promise[String]
		send( createSubmitJobMessage( funcName, uid, data, callback.isEmpty, priority ), 
			timeout, 
			createJobResponseChecker( callback, p ) )
		p.future 
	}

	
	/**
	 * submit a background job with {@code funcName}, {@code uid}, {@code data}, 
	 * {@code priority} and {@code timeout} to server
	 * 
	 * @param funcName the function name
	 * @param uid the unique identifier
	 * @param data the data
	 * @param priority the job priority {#link JobPriority}
	 * @param timeout > 0 the timeout in seconds, <= 0 no timeout
	 * @return a Future[String] contains the returned job handler	 	 	 	 	  	 	 
	 */	 	
	def submitJobBg( funcName: String, uid: String, data: String, priority:JobPriority , timeout: Int ):Future[ String ] = submitJob( funcName, uid, data, priority, timeout, None )
	
	/**
	 * submit a background job with {@code funcName}, {@code uid} and {@code data} to server
	 * 
	 * The submitted job has Normal priority {#link JobPriority} and no timeout	 	  
	 * 
	 * @param funcName the function name
	 * @param uid the unique identifier
	 * @param data the data
	 * @return a Future[String] contains the returned job handler	 	 	 	 	  	 	 
	 */	
	def submitJobBg( funcName: String, uid: String, data: String ): Future[ String ] = submitJob( funcName, uid, data, JobPriority.Normal, -1, None )
	
	/**
	 * submit a background job with {@code funcName}, {@code uid}, {@code data}, 
	 * {@code priority} to server
	 * 
	 * No timeout for the submitted job	 	 
	 * 
	 * @param funcName the function name
	 * @param uid the unique identifier
	 * @param data the data
	 * @param priority the job priority {#link JobPriority}	 
	 * @return a Future[String] contains the returned job handler	 	 	 	 	  	 	 
	 */	 	
	def submitJobBg( funcName: String, uid: String, data: String, priority:JobPriority ): Future[ String ] = submitJob( funcName, uid, data, priority, -1, None ) 
	
	/**
	 * submit a job with {@code funcName}, {@code uid}, {@code data}, 
	 * {@code priority}, {@code timeout} and {@code callback} to server
	 * 
	 * The callback will receive the data sent by the worker	 	 
	 * 
	 * @param funcName the function name
	 * @param uid the unique identifier
	 * @param data the data
	 * @param priority the job priority {#link JobPriority}
	 * @param timeout > 0 the timeout in seconds, <= 0 no timeout
	 * @param callback to receive the data from worker
	 * 	 	 
	 * @return a Future[String] contains the returned job handler	 	 	 	 	  	 	 
	 */	 	
	def submitJob( funcName: String, uid: String, data: String, priority:JobPriority, timeout: Int )( jobCallback: JobEvent=>Unit ): Future[ String ] = submitJob( funcName, uid, data, priority, timeout, Some( jobCallback ) )

    /**
	 * submit a job with {@code funcName}, {@code uid}, {@code data}, 
	 * and {@code callback} to server
	 *
	 * The submitted job has a Normal priority {#link JobPriority} and no timeout
	 * 	  	  
	 * The callback will receive the data sent by the worker	 	 
	 * 
	 * @param funcName the function name
	 * @param uid the unique identifier
	 * @param data the data
	 * @param callback to receive the data from worker
	 * 	 	 
	 * @return a Future[String] contains the returned job handler	 	 	 	 	  	 	 
	 */	 
	def submitJob( funcName: String, uid: String, data: String )( jobCallback: JobEvent=>Unit  ): Future[ String ] = submitJob( funcName, uid, data, JobPriority.Normal, -1, Some( jobCallback ) )
	
	/**
	 * submit a job with {@code funcName}, {@code uid}, {@code data}, 
	 * {@code priority}, {@code timeout} and {@code callback} to server
	 * 
	 * The submitted job has no timeout
	 * 	 	 
	 * The callback will receive the data sent by the worker	 	 
	 * 
	 * @param funcName the function name
	 * @param uid the unique identifier
	 * @param data the data
	 * @param priority the job priority {#link JobPriority}
	 * @param timeout > 0 the timeout in seconds, <= 0 no timeout
	 * @param callback to receive the data from worker
	 * 	 	 
	 * @return a Future[String] contains the returned job handler	 	 	 	 	  	 	 
	 */	 
	def submitJob( funcName: String, uid: String, data: String, priority:JobPriority )( jobCallback: JobEvent=>Unit ): Future[ String ] = submitJob( funcName, uid, data, priority, -1, Some(jobCallback ) ) 

	/**
	 *  get the job status
	 *  
	 * @param jobHandle the job handle
	 * @param timeout the optional timeout, if no timeout is provided, the defMsgTimeout
	 * will be used
	 * 
	 * @return the job status	 	 
	 */
	def getStatus(  jobHandle: String, timeout: Int = -1 ) : Future[ JobStatus ] = {
		val p = Promise[ JobStatus]
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
			}})
		
		p.future		
	}
	
	/**
	 *  do graceful shutdown
	 */	 	
	def shutdown {
		stopped = true
		val p = Promise[ Boolean ]
		executor.submit( new Runnable {
			def run {
				if( pendingJobs.size > 0 || runningJobs.size > 0 ) {
					executor.submit( this )
				} else {
					p success true
				} 
			}
		} )
		
		Await.ready( p.future, Duration.Inf )
	}

	private def createSubmitJobMessage( funcName: String, uid: String, data: String, background: Boolean, priority:JobPriority ) = {
		priority match {
			case JobPriority.Normal => if( background ) SubmitJobBg( funcName, uid, data ) else SubmitJob( funcName, uid, data )
			case JobPriority.High => if( background ) SubmitJobHighBg( funcName, uid, data ) else SubmitJobHigh( funcName, uid, data ) 
			case _ => if( background ) SubmitJobLowBg( funcName, uid, data ) else SubmitJobLow( funcName, uid, data )
		}
	}
	private def start( index: Int ) {
		if( index >= serverAddrs.size ) {
			start( 0 )
		} else {
			val msgHandler = new MessageHandler {
				override def handleMessage( msg: Message, from: MessageChannel ) {
					doResponseCheck( msg )
				}
				
				override def handleDisconnect( from: MessageChannel ) {
					clientChannel = null
					notifyConnectionLost
					start( 0 )
				}
			}
			
			val callback = { channel: MessageChannel =>
				if( channel != null ) {
					clientChannel = channel
					channel.setMessageHandler( msgHandler )
					channel.open
					sendPendingJobs
				} else start( index + 1 ) 
			}
			
			AsyncSockMessageChannel.asyncConnect( serverAddrs( index ), callback, Some( executor ) )			
		}
	}
			
	
	private def createJobResponseChecker( callback: Option[JobEvent=>Unit], p: Promise[ String ] )  =  new ResponseChecker {
		@volatile
		var thisJobHandle: String = null
		
		def checkResponse( msg: Option[Message], connLost: Boolean, timeout: Boolean ): ResponseCheckResult = {
			if( connLost ) {
				callback.get( JobConnectionLost() )
				ResponseCheckResult( true, true )
			} else if( timeout ) {
             	callback.get( JobTimeout() )
             	ResponseCheckResult( true, true )
            } else if( msg.isEmpty ) {
				ResponseCheckResult( false, false ) 
			} else msg.get match {
					case JobCreated( jobHandle ) =>						
						thisJobHandle = jobHandle
						p success jobHandle
						if( callback.isEmpty ) ResponseCheckResult( true, true ) else ResponseCheckResult( true, false )
					case WorkDataRes( jobHandle, data ) =>
						if( jobHandle == thisJobHandle ) {
							callback.get( JobData( data ) )
							ResponseCheckResult( true, false )
						} else ResponseCheckResult( false, false )  
					case WorkWarningRes( jobHandle, data ) =>
						if( jobHandle == thisJobHandle ) {
							callback.get( JobWarning( data ) )
							ResponseCheckResult( true, false )
						} else ResponseCheckResult( false, false )
					case WorkStatusRes( jobHandle, numerator, denominator ) =>
						if( jobHandle == thisJobHandle ) {
							callback.get( JobStatus( true, true, numerator, numerator ) )
							ResponseCheckResult( true, false )
						} else ResponseCheckResult( false, false )
					case WorkCompleteRes( jobHandle, data ) =>
					    if( jobHandle == thisJobHandle ) {
							callback.get( JobComplete( data ) )
							ResponseCheckResult( true, true )
						} else ResponseCheckResult( false, false )
					case WorkFailRes( jobHandle ) =>
						if( jobHandle == thisJobHandle ) {
							callback.get( JobFail() )
							ResponseCheckResult( true, true )
						} else ResponseCheckResult( false, false )
					case WorkExceptionRes( jobHandle, data ) =>
						if( jobHandle == thisJobHandle ) {
							callback.get( JobException( data ) )
							ResponseCheckResult( true, false )
						} else ResponseCheckResult( false, false ) 	
					case _ => ResponseCheckResult( false, false )
					
				}
		}
	}
	
	private def sendPendingJobs {
		if(  clientChannel != null && pendingJobs.size > 0 && ( maxOnGoingJobs <= 0 || runningJobs.size < maxOnGoingJobs ) ) {
			val jobInfo = pendingJobs.removeFirst					
			runningJobs.add( jobInfo )			

			clientChannel.send( jobInfo.msg, Some( success => {if( !success ) pendingJobs.add( jobInfo )} ) )
		}
	}
	
	
	private def send( msg: Message, timeout:Int, respChecker: ResponseChecker  ) {
		if( stopped ) {		
			throw new Exception( "in shutdown, no message will be sent to server")
			return
		}
		val jobInfo = JobInfo( msg, timeout, respChecker )
		executor.submit( new Runnable {
			def run {
				if( timeout > 0 ) timer.schedule( new TimerTask {
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
				pendingJobs.add( jobInfo )
				sendPendingJobs
			}
		})		
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
	
	private def parseServers = parseAddressList( servers )
}

object GearmanClient {
	def apply( servers: String, maxOnGoingJobs: Int = 10  ) = new GearmanClient( servers, maxOnGoingJobs )
}



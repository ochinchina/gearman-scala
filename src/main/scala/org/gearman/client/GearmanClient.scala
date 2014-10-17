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
import ExecutionContext.Implicits.global

/**
 * the job callback. When gearman client submits a job to gearman server, a job
 * callback must be provided to receive job related data. 
 * 
 * @author Steven Ou  
 */ 
trait JobCallback {
	/**
	 * notify the callback data is received from the worker
	 * 	 
	 * @param data received from the worker
	 *  
	 */	 	
	def data( data: String )
	
	/**
	 * notify the callback warning is received from the worker
	 * 	 
	 * @param warning received from the worker
	 *  
	 */
	def warning( data: String )
	
	/**
	 * notify the callback job status is updated
	 * 	 
	 * @param numerator the complete percentage of numerator
	 * @param denominator the complete percentage of denominator 
	 */
	def status( numerator: Int, denominator: Int )
	
	/**
	 * notify the callback job is completed
	 * 	 
	 * @param data received from the worker 
	 */
	def complete( data: String ) 
	
	/**
	 * notify the callback job is failed
	 * 	 
	 */
	def fail

	/**
	 * notify the callback exception occurs about the job
	 * 	 
	 */
	def exception( data: String )
	
	/**
	 * notify the callback connection is lost
	 */
	def connectionLost
	
	/**
	 *  notify the callback the job is timeout
	 */	 	
	def timeout
}

class JobCallbackProxy( callback: JobCallback ) extends JobCallback {
	override def data( data: String ) {
		future { callback.data( data) }
	}
	
	override def warning( data: String ) {
		future { callback.warning( data) } 
	}
	
	override def status( numerator: Int, denominator: Int ) {
		future { callback.status( numerator, denominator) } 
	}
	
	override def complete( data: String )  {
		future { callback.complete( data ) } 
	}
	
	override def fail {
		future { callback.fail } 
	}
	
	override def exception( data: String ) {
		future { callback.exception( data ) } 
	}
	
	override def connectionLost {
		future { callback.connectionLost } 
	}
	
	override def timeout {
		future { callback.timeout } 
	} 
}

/**
 *  construct a GearmanClient object
 *                    
 * @param servers the gearman server address list, the address list is in
 * "server1:port,server2:port,...,servern:port"
 * @param maxOnGoingJobs the max number of jobs can be sent to gearman server
 * 
 * @param defMsgTimeout > 0 default message timeout in milliseconds, <= 0 no timeout      
 */ 
class GearmanClient( servers: String, maxOnGoingJobs: Int = 10, defMsgTimeout: Int = -1 ) {
	import org.gearman.message.JobPriority._
	import Array._
	
	val serverAddrs = parseServers
	var stopped = false
	
	var clientChannel: MessageChannel = null	
	val runningJobs = new LinkedList[JobInfo]
	val pendingJobs = new LinkedList[JobInfo]
	val executor = Executors.newFixedThreadPool( 1 )
	val timer = new Timer
	
	start( 0 ) 

	case class JobInfo( msg: Message, timeout: Int, respChecker: ResponseChecker )
	case class ResponseCheckResult( isMyResponse: Boolean, finished: Boolean )
	
	trait ResponseChecker {
		def checkResponse( msg: Option[Message], connLost: Boolean, timeout: Boolean ): ResponseCheckResult
	}
	
	
	case class Response[T](var value: T=null, var connLost: Boolean = false, var timeout: Boolean = false) {
		val valueNotifier = new ValueNotifier[T] 
		
		def waitValue{
			valueNotifier.waitValue
		}
		
		def notifyValue {
			valueNotifier.notifyValue( value )
		}
		
		def returnValue: T = {
			if( connLost )
				throw new Exception("Communication Lost")
			else if( timeout )
				throw new Exception( "Timeout")
			else value 
		}
		
	}
	
	abstract class AbsResponseChecker[T]( resp: Response[T]) extends ResponseChecker {		 		
		override def checkResponse( msg: Option[Message], connLost: Boolean, timeout: Boolean ): ResponseCheckResult = {
			if( connLost ) {
				resp.connLost = true
				resp.notifyValue
				new ResponseCheckResult( true, true ) 				
			} else if( timeout ) {
				resp.timeout = true
				resp.notifyValue
				new ResponseCheckResult( true, true )
			} else msg match {
				case Some( respMsg: Message ) => checkResponse( respMsg )
				case _ => new ResponseCheckResult( false, false )
			}
		}
		
		def checkResponse( msg: Message ): ResponseCheckResult
		
	}
	
	
	/**
	 *  execute ECHO message in async mode
	 *  
	 * @param data the data sent to the server
	 * @param callback the callback to receive the echoed data
	 * @param timeout optional timeout in seconds	 	 	 	 
	 */	 	
	def asyncEcho( data: String, callback: String => Unit, timeout: Option[Int] = None ) {
		val resp = new Response[String]
		
		send( new EchoReq( data ), timeout.getOrElse( defMsgTimeout ), new AbsResponseChecker[String]( resp ) {
			override def checkResponse( msg: Message ): ResponseCheckResult = {
				msg match {
					case EchoRes( respData ) =>
						resp.value = respData
						future { callback( respData ) }
						new ResponseCheckResult( true, true )
					case _ => new ResponseCheckResult( false, false )
				}
			}
		})
	}
	
	/**
	 *  execute ECHO message
	 *  
	 * @param data the data sent to the server
	 * @param timeout optional timeout in seconds	 	 	 	 
	 */	 	
	def echo( data: String, timeout: Option[Int] = None ):String = {
		val resp = new Response[String]
		
		asyncEcho( data, {respData => resp.value = respData; resp.notifyValue }, timeout )
				
		resp.waitValue
		resp.returnValue 
	}
		
	
	/**
	 *  submit a job to the server in async mode
	 *  
	 * @param funcName the function name
	 * @param uid unique identifier
	 * @param data the function data
	 * @param jobCreatedCallback the callback to receive the handle of job assigned by gearman server	 
	 * @param callback the optional callback used to receive the job status, 
	 * 	 error, exception, data and completion info. If no callback is provided,
	 * 	 the job will be submitted as background job	  
	 * @param priority the job priority, must be Normal, High or Low, default is
	 * 	Normal	 
	 * @param timeout the optional timeout in seconds	 	  	 	 	 	 	 
	 */	 	
	def asyncSubmitJob( funcName: String, uid: String, data: String, jobCreatedCallback: Option[ String => Unit ] = None, callback: Option[JobCallback] = None, priority:JobPriority = JobPriority.Normal, timeout: Option[Int] = None ) {		
		send( createSubmitJobMessage( funcName, uid, data, callback, priority ), timeout.getOrElse( defMsgTimeout ), createJobResponseChecker( if( callback.isEmpty ) None else Some( new JobCallbackProxy( callback.get ) ), jobCreatedCallback ) )			
	}
	
	/**
	 *  submit a job to the server
	 *  
	 * @param funcName the function name
	 * @param uid unique identifier
	 * @param data the function data
	 * @param callback the optional callback used to receive the job status, 
	 * 	 error, exception, data and completion info. If no callback is provided,
	 * 	 the job will be submitted as background job	  
	 * @param priority the job priority, must be Normal, High or Low, default is
	 * 	Normal	 
	 * @param timeout the optional timeout in seconds
	 * 
	 * @return the job handle assigned by job server	 	 	 	  	 	 	 	 	 
	 */	 	
	def submitJob( funcName: String, uid: String, data: String, callback: Option[JobCallback] = None, priority:JobPriority = JobPriority.Normal, timeout: Option[Int] = None ) : String = {
		val resp = new Response[String]
				
		asyncSubmitJob( funcName, uid, data, Some{ case( jobHandle ) => resp.value = jobHandle; resp.notifyValue }, callback, priority, timeout )
		resp.waitValue
		resp.returnValue
	}
	
	/**
	 *  get the job status in async mode
	 *  
	 * @param jobHandle the job handle
	 * @param callback the job status callback
	 * @param timeout the optional timeout, if no timeout is provided, the defMsgTimeout
	 * will be used
	 */	 		
	def asyncGetStatus( jobHandle: String, callback: StatusRes=>Unit, timeout: Option[Int] = None ) {
		val resp = new Response[StatusRes]
		
		send( new GetStatus( jobHandle), timeout.getOrElse( defMsgTimeout ), new AbsResponseChecker[StatusRes]( resp ) {
			override def checkResponse( msg: Message ): ResponseCheckResult = {
				if( msg.isInstanceOf[StatusRes]) {
					resp.value = msg.asInstanceOf[ StatusRes ]
					future { callback( resp.value ) }
					if( resp.value.jobHandle == jobHandle ) new ResponseCheckResult( true, true ) else new ResponseCheckResult( false, false )
				} else new ResponseCheckResult( false, false )
			}
		}) 
	}
	
	/**
	 *  get the job status
	 *  
	 * @param jobHandle the job handle
	 * @param timeout the optional timeout, if no timeout is provided, the defMsgTimeout
	 * will be used
	 * 
	 * @return the job status	 	 
	 */
	def getStatus(  jobHandle: String, timeout: Option[Int] = None ) : StatusRes = {
		val resp = new Response[StatusRes]
		
		asyncGetStatus( jobHandle, { status => resp.value = status; resp.notifyValue }, timeout )
		resp.waitValue
		resp.returnValue
	}
	
	/**
	 *  do graceful shutdown
	 */	 	
	def shutdown {
		stopped = true
		val valueNotifier = new ValueNotifier[ Boolean ]
		executor.submit( new Runnable {
			def run {
				if( pendingJobs.size > 0 || runningJobs.size > 0 ) {
					executor.submit( this )
				} else {
					valueNotifier.notifyValue( true )
				} 
			}
		} )
		
		valueNotifier.waitValue
	}

	private def createSubmitJobMessage( funcName: String, uid: String, data: String, callback: Option[JobCallback], priority:JobPriority ) = {
		priority match {
			case JobPriority.Normal => if( callback.isEmpty ) new SubmitJobBg( funcName, uid, data ) else new SubmitJob( funcName, uid, data )
			case JobPriority.High => if( callback.isEmpty ) new SubmitJobHighBg( funcName, uid, data ) else new SubmitJobHigh( funcName, uid, data ) 
			case _ => if( callback.isEmpty ) new SubmitJobLowBg( funcName, uid, data ) else new SubmitJobLow( funcName, uid, data )
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
			
	
	private def createJobResponseChecker( callback: Option[JobCallback], jobCreatedCallback: Option[ String => Unit ] )  =  new ResponseChecker {
		@volatile
		var thisJobHandle: String = null
		
		def checkResponse( msg: Option[Message], connLost: Boolean, timeout: Boolean ): ResponseCheckResult = {
			if( connLost ) {
				callback.get.connectionLost
				new ResponseCheckResult( true, true )
			} else if( timeout ) {
             	callback.get.timeout
             	new ResponseCheckResult( true, true )
            } else if( msg.isEmpty ) {
				new ResponseCheckResult( false, false ) 
			} else msg.get match {
					case JobCreated( jobHandle ) =>
						thisJobHandle = jobHandle
						if( jobCreatedCallback.nonEmpty ) future { jobCreatedCallback.get( jobHandle ) }
						if( callback.nonEmpty ) new ResponseCheckResult( true, false ) else new ResponseCheckResult( true, true )
					case WorkDataRes( jobHandle, data ) =>
						if( jobHandle == thisJobHandle ) {
							callback.get.data( data )
							new ResponseCheckResult( true, false )
						} else new ResponseCheckResult( false, false )  
					case WorkWarningRes( jobHandle, data ) =>
						if( jobHandle == thisJobHandle ) {
							callback.get.warning( data )
							new ResponseCheckResult( true, false )
						} else new ResponseCheckResult( false, false )
					case WorkStatusRes( jobHandle, numerator, denominator ) =>
						if( jobHandle == thisJobHandle ) {
							callback.get.status( numerator, numerator )
							new ResponseCheckResult( true, false )
						} else new ResponseCheckResult( false, false )
					case WorkCompleteRes( jobHandle, data ) =>
					    if( jobHandle == thisJobHandle ) {
							callback.get.complete( data )
							new ResponseCheckResult( true, true )
						} else new ResponseCheckResult( false, false )
					case WorkFailRes( jobHandle ) =>
						if( jobHandle == thisJobHandle ) {
							callback.get.fail
							new ResponseCheckResult( true, true )
						} else new ResponseCheckResult( false, false )
					case WorkExceptionRes( jobHandle, data ) =>
						if( jobHandle == thisJobHandle ) {
							callback.get.exception( data )
							new ResponseCheckResult( true, false )
						} else new ResponseCheckResult( false, false ) 	
					case _ => new ResponseCheckResult( false, false )
					
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
		val jobInfo = new JobInfo( msg, timeout, respChecker )
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
	def apply( servers: String, maxOnGoingJobs: Int = 10, defMsgTimeout: Int = -1  ) = new GearmanClient( servers, maxOnGoingJobs, defMsgTimeout )
}



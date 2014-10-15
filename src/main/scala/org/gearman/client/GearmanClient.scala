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
		try { callback.timeout } 
	} 
}

/**
 *  construct a GearmanClient object
 *                    
 * @param servers the gearman server address list, the address list is in
 * "server1:port,server2:port,...,servern:port"
 * @param maxOnGoingJobs the max number of jobs can be sent to gearman server
 * 
 * @param defMsgTimeout default message timeout in milliseconds      
 */ 
class GearmanClient( servers: String, maxOnGoingJobs: Int = 10, defMsgTimeout: Int = 10000 ) {
	import Array._
	
	val serverAddrs = parseServers
	
	var clientChannel: MessageChannel = null	
	val runningJobs = new LinkedList[JobInfo]
	val pendingJobs = new LinkedList[JobInfo]
	val executor = Executors.newFixedThreadPool( 1 )
	val timer = new Timer 

	case class JobInfo( msg: Message, timeout: Int, respChecker: ResponseChecker )
	case class ResponseCheckResult( isMyResponse: Boolean, finished: Boolean )
	
	trait ResponseChecker {
		def checkResponse( msg: Option[Message], connLost: Boolean, timeout: Boolean ): ResponseCheckResult
	}
	
	
	case class Response[T]( var value: T=null, var connLost: Boolean = false, var timeout: Boolean = false) {
		val valueNotifier = new ValueNotifier[T] 
		
		def waitValue {
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
	
	
	def echo( data: String, timeout: Option[Int] = None ):String = {
		val resp = new Response[String]
		
		send( new EchoReq( data ), timeout.getOrElse( defMsgTimeout ), new AbsResponseChecker[String]( resp ) {
			override def checkResponse( msg: Message ): ResponseCheckResult = {
				msg match {
					case EchoRes( respData ) =>
						resp.value = respData
						resp.notifyValue
						new ResponseCheckResult( true, true )
					case _ => new ResponseCheckResult( false, false )
				}
			}
		})
		
		resp.waitValue
		resp.returnValue 
	}
		
	
	def submitJob( funcName: String, uid: String, data: String, callback: JobCallback, timeout: Option[Int] = None ) {
		send( new SubmitJob( funcName, uid, data ), timeout.getOrElse( defMsgTimeout ), createJobResponseChecker( new JobCallbackProxy( callback ) ) )			
	}

	def start {
		if( serverAddrs.size > 0 ) start( 0 )
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
			
	
	private def createJobResponseChecker( callback: JobCallback )  =  new ResponseChecker {
		@volatile
		var thisJobHandle: String = null
		
		def checkResponse( msg: Option[Message], connLost: Boolean, timeout: Boolean ): ResponseCheckResult = {
			if( connLost ) {
				callback.connectionLost
				new ResponseCheckResult( true, true )
			} else if( timeout ) {
             	callback.timeout
             	new ResponseCheckResult( true, true )
            } else if( msg.isEmpty ) {
				new ResponseCheckResult( false, false ) 
			} else msg.get match {
					case JobCreated( jobHandle ) =>
						thisJobHandle = jobHandle
						new ResponseCheckResult( true, false )
					case WorkDataRes( jobHandle, data ) =>
						if( jobHandle == thisJobHandle ) {
							callback.data( data )
							new ResponseCheckResult( true, false )
						} else new ResponseCheckResult( false, false )  
					case WorkWarningRes( jobHandle, data ) =>
						if( jobHandle == thisJobHandle ) {
							callback.warning( data )
							new ResponseCheckResult( true, false )
						} else new ResponseCheckResult( false, false )
					case WorkStatusRes( jobHandle, numerator, denominator ) =>
						if( jobHandle == thisJobHandle ) {
							callback.status( numerator, numerator )
							new ResponseCheckResult( true, false )
						} else new ResponseCheckResult( false, false )
					case WorkCompleteRes( jobHandle, data ) =>
					    if( jobHandle == thisJobHandle ) {
							callback.complete( data )
							new ResponseCheckResult( true, true )
						} else new ResponseCheckResult( false, false )
					case WorkFailRes( jobHandle ) =>
						if( jobHandle == thisJobHandle ) {
							callback.fail
							new ResponseCheckResult( true, true )
						} else new ResponseCheckResult( false, false )
					case WorkExceptionRes( jobHandle, data ) =>
						if( jobHandle == thisJobHandle ) {
							callback.exception( data )
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
		val jobInfo = new JobInfo( msg, timeout, respChecker )
		executor.submit( new Runnable {
			def run {
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



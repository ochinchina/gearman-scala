package org.gearman.client

import org.gearman.message._
import org.gearman.channel._
import org.gearman.util.Util._
import java.util.{ LinkedList }
import scala.util.control.Breaks._
import java.nio.channels.{AsynchronousSocketChannel,
						CompletionHandler}
						
import java.net.{InetSocketAddress}


trait JobCallback {
	def data( data: String )
	def warning( data: String )
	def status( numerator: Int, denominator: Int )
	def complete( data: String )
	def fail
	def exception( data: String )
}

/**
 *  construct a GearmanClient object
 *                    
 * @param servers the gearman server address list, the address list is in
 * "server1:port,server2:port,...,servern:port"    
 */ 
class GearmanClient( servers: String ) {
	import Array._
	
	val serverAddrs = parseServers
	
	var clientChannel:MessageChannel = null
	val respCheckers = new LinkedList[ResponseChecker] 

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
	
	
	def echo( data: String ):String = {
		val resp = new Response[String]
		
		send( new EchoReq( data ), new AbsResponseChecker[String]( resp ) {
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
		
	
	def submitJob( funcName: String, uid: String, data: String, callback: JobCallback ) {
		val resp = new Response[String]
		
		send( new SubmitJob( funcName, uid, data ), new AbsResponseChecker[String]( resp ) {
			override def checkResponse( msg: Message ): ResponseCheckResult = {
				msg match {
					case JobCreated( jobHandle ) =>
						resp.value = jobHandle
						new ResponseCheckResult( true, false )
					case WorkDataRes( jobHandle, data ) =>
						if( jobHandle == resp.value ) {
							callback.data( data )
							new ResponseCheckResult( true, false )
						} else new ResponseCheckResult( false, false )  
					case WorkWarningRes( jobHandle, data ) =>
						if( jobHandle == resp.value ) {
							callback.warning( data )
							new ResponseCheckResult( true, false )
						} else new ResponseCheckResult( false, false )
					case WorkStatusRes( jobHandle, numerator, denominator ) =>
						if( jobHandle == resp.value ) {
							callback.status( numerator, numerator )
							new ResponseCheckResult( true, false )
						} else new ResponseCheckResult( false, false )
					case WorkCompleteRes( jobHandle, data ) =>
					    if( jobHandle == resp.value ) {
							callback.complete( data )
							new ResponseCheckResult( true, true )
						} else new ResponseCheckResult( false, false )
					case WorkFailRes( jobHandle ) =>
						if( jobHandle == resp.value ) {
							callback.fail
							new ResponseCheckResult( true, true )
						} else new ResponseCheckResult( false, false )
					case WorkExceptionRes( jobHandle, data ) =>
						if( jobHandle == resp.value ) {
							callback.exception( data )
							new ResponseCheckResult( true, false )
						} else new ResponseCheckResult( false, false ) 	
					case _ => new ResponseCheckResult( false, false )
					
				}
			}
		} )				
	}
	
	def start {

		breakable {
			while( true ) {		
				for( i <- 0 until serverAddrs.size ) {
					clientChannel = AsyncSockMessageChannel.connect( serverAddrs( i ) )
					if( clientChannel != null ) break
				}
			}
		}

		clientChannel.setMessageHandler( new MessageHandler {
			override def handleMessage( msg: Message, from: MessageChannel ) {
				doResponseCheck( msg )
			}
			
			override def handleDisconnect( from: MessageChannel ) {
				notifyConnectionLost
				start
			}
		} )
		clientChannel.start
	}
	
	
	
	
	private def send( msg: Message, respChecker: ResponseChecker  ) {
		respCheckers.synchronized {
			respCheckers.add( respChecker )
		}
		clientChannel.send( msg )
	}
	
	private def doResponseCheck(msg:Message) {
		respCheckers.synchronized {
			val iter = respCheckers.iterator
	
			breakable {		
				while( iter.hasNext ) {
					val respChecker = iter.next
					val checkResult = respChecker.checkResponse( Some( msg ), false, false )
					if( checkResult.isMyResponse ) {
						if( checkResult.finished ) respCheckers.remove( respChecker )
						break
					} 
				}
			}
		}
		
	}
	
	private def notifyConnectionLost() {
		respCheckers.synchronized {
			val iter = respCheckers.iterator
			while( iter.hasNext ) {
				iter.next.checkResponse( None, true, false )
			}
			respCheckers.clear
		}		
	}
	
	private def parseServers = parseAddressList( servers )
}


object GearmanClient {
	def main( args: Array[String] ) {
		val client = new GearmanClient( "127.0.0.1:3333,127.0.0.1:3334" )
		client.start
		println( client.echo( "test") )
		
		client.submitJob( "test", "1234", "hello", new JobCallback {
			def data( data: String ) {
				println( "data:" + data )
			}
			def warning( data: String ) {
			}
			def status( numerator: Int, denominator: Int ) {
			}
			def complete( data: String ) {
				println( "complete:" + data )
				client.submitJob( "test", "12345", "hello", this )
			}
			def fail {
			}
			def exception( data: String ) {
			}
		})
		
		while( true ) {
			Thread.sleep( 1000 )
		}
	}
}
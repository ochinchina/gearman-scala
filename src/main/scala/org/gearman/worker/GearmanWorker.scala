package org.gearman.worker

import org.gearman.message._
import org.gearman.channel._
import org.gearman.util.Util._
import scala.collection.mutable.{HashMap}
import scala.util.control.Breaks._
import java.net.InetSocketAddress

trait WorkResponser {
	def data( data: String	 )
	def status( numerator: Int, denominator: Int )
	def warning( data: String )
	def complete( data: String )
	def fail
	def exception( data: String )
}

trait WorkFuncHandler {
	def handle( jobHandle: String, 
				funcName: String, 
				data: String, 
				uid: Option[String],
				responser: WorkResponser )
}

class DefWorkResponser( jobHandle: String, channel: MessageChannel ) extends WorkResponser {

	def data( data: String ) {
		channel.send( new WorkDataReq( jobHandle, data ) )
	}
	
	def status( numerator: Int, denominator: Int ) {
		channel.send( new WorkStatusReq( jobHandle, numerator, denominator ) )
	}
	
	def complete( data: String ) {
		channel.send( new WorkCompleteReq( jobHandle, data ) )
	}
	def warning( data: String ) {
		channel.send( new WorkWarningReq( jobHandle, data ) )
	}
	
	def fail {
		channel.send( new WorkFailReq( jobHandle ) )
	}
	
	def exception( data: String ) {
		channel.send( new WorkExceptionReq( jobHandle, data ) )
	}
}

class GearmanWorker( servers: String ) {
	val funcHandlers = new HashMap[ String, WorkFuncHandler ]
	val serverAddrs = parseAddressList( servers )
	val channels = new java.util.LinkedList[MessageChannel]
		
	def registerHandler( funcName: String, handler: WorkFuncHandler ) {
		funcHandlers.synchronized { funcHandlers += funcName -> handler }
		broadcast( new CanDo( funcName ) )
	}
	def unregisterHandler( funcName: String ) {
		funcHandlers.synchronized { funcHandlers -= funcName }
		broadcast( new CantDo( funcName ) )
	}	
	
	def start() {		
		for( i <- 0 until serverAddrs.size ) start( serverAddrs( i ) )			
	}
	
	private def start( addr: InetSocketAddress ) {		
		AsyncSockMessageChannel.asyncConnect( addr, channel => {
			if( channel != null ) {
				channels.synchronized { channels.add( channel ) }
				channel.setMessageHandler( createMessageHandler( addr ) )
				funcHandlers.synchronized { 		
					funcHandlers.foreach( p => channel.send( new CanDo( p._1 ) ) )
				}				
				channel.start						
				channel.send( new GrabJob )	
			}
		} )
	}
	
	private def createMessageHandler( addr: InetSocketAddress ) = new MessageHandler {
		override def handleMessage( msg: Message, from: MessageChannel ) {
			msg match {
				case JobAssign( jobHandle, funcName, data ) =>
					funcHandlers.get( funcName ) match {
						case Some( handler ) => handler.handle( jobHandle, funcName, data, None, new DefWorkResponser( jobHandle, from ) )
						case _ => from.send( new Error( "2", "No handler found") )
					}
					from.send( new GrabJob )
				case JobAssignUniq( jobHandle, funcName, uid, data ) =>
					funcHandlers.get( funcName ) match {
						case Some( handler ) => handler.handle( jobHandle, funcName, data, Some( uid ), new DefWorkResponser( jobHandle, from ) )
						case _ => from.send( new Error( "2", "No handler found") )
					}
					from.send( new GrabJob )
				case Noop() => from.send( new GrabJob )
				case NoJob() => from.send( new PreSleep )																		
				case _ => from.send( new PreSleep )
			} 
		}
		
		override def handleDisconnect( from: MessageChannel ) {
			channels.synchronized { channels.remove( from ) }
			start( addr )
		}
	}
	
	private def broadcast( msg: Message ) {
		channels.synchronized {
			val iter = channels.iterator
			while( iter.hasNext ) {
				iter.next.send( msg )
			}
		}
	} 
}

object GearmanWorker {
	def main( args: Array[String] ) {
		val worker = new GearmanWorker( "127.0.0.1:3333,127.0.0.1:3334" )
		worker.registerHandler( "test", new WorkFuncHandler {
			def handle( jobHandle: String, 
				funcName: String, 
				data: String, 
				uid: Option[String],
				responser: WorkResponser ) {
					responser.complete( "no data" )
				}
		})
		
		worker.start
		
		while( true ) {
			Thread.sleep( 1000 )
		}		
	}
}
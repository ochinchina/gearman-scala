package org.gearman

import org.gearman.message._
import org.gearman.channel._
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

class GearmanWorker( server: String, port: Int ) extends MessageHandler {
	var channel: MessageChannel = null 
	val funcHandlers = new HashMap[ String, WorkFuncHandler ]
	
	
	def registerHandler( funcName: String, handler: WorkFuncHandler ) {
		funcHandlers += funcName -> handler
		if( channel != null ) channel.send( new CanDo( funcName ) )
	}
	def unregisterHandler( funcName: String ) {
		funcHandlers -= funcName
		if( channel != null ) channel.send( new CantDo( funcName ) )
	}	
	
	def start() {
		channel = AsyncSockMessageChannel.connect( new InetSocketAddress( server, port ) )
		channel.setMessageHandler( this )
		
		funcHandlers.foreach( p => channel.send( new CanDo( p._1 ) ) )
		
		channel.start
				
		channel.send( new GrabJob )		
	}
	
	override def handleMessage( msg: Message, from: MessageChannel ) {
		msg match {
			case JobAssign( jobHandle, funcName, data ) =>
				funcHandlers.get( funcName ) match {
					case Some( handler ) => 
						handler.handle( jobHandle, funcName, data, None, new DefWorkResponser( jobHandle, channel ) )
					case _ => 
						channel.send( new Error( "2", "No handler found") )
				}
				channel.send( new GrabJob )
			case JobAssignUniq( jobHandle, funcName, uid, data ) =>
				funcHandlers.get( funcName ) match {
					case Some( handler ) => 
						handler.handle( jobHandle, funcName, data, Some( uid ), new DefWorkResponser( jobHandle, channel ) )
					case _ => 
						channel.send( new Error( "2", "No handler found") )
				}
				channel.send( new GrabJob )
			case Noop() => channel.send( new GrabJob )
			case NoJob() => channel.send( new PreSleep )																		
			case _ => channel.send( new PreSleep )
		} 
	}
	
	override def handleDisconnect( from: MessageChannel ) {
		start
	}
}

object GearmanWorker {
	def main( args: Array[String] ) {
		val worker = new GearmanWorker( "127.0.0.1", 3333 )
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
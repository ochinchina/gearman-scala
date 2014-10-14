package org.gearman.worker

import org.gearman.message._
import org.gearman.channel._
import org.gearman.util.Util._
import scala.collection.mutable.{HashMap}
import scala.util.control.Breaks._
import java.net.InetSocketAddress
import scala.concurrent._
import ExecutionContext.Implicits.global

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

class JobList {
	val jobs = new HashMap[ String, MessageChannel ]
	
	def addJob( jobHandle: String, channel: MessageChannel ) {
		jobs.synchronized { jobs += jobHandle -> channel }
	}
	
	def removeJob( jobHandle: String ) {
		jobs.synchronized { jobs -= jobHandle }
	}
	
	def size = jobs.synchronized{ jobs.size }

}

class DefWorkResponser( jobHandle: String, channel: MessageChannel, jobCompleted: =>Unit ) extends WorkResponser {

	def data( data: String ) {
		channel.send( new WorkDataReq( jobHandle, data ) )
	}
	
	def status( numerator: Int, denominator: Int ) {
		channel.send( new WorkStatusReq( jobHandle, numerator, denominator ) )
	}
	
	def complete( data: String ) {
		jobCompleted
		channel.send( new WorkCompleteReq( jobHandle, data ) )
	}
	def warning( data: String ) {
		channel.send( new WorkWarningReq( jobHandle, data ) )
	}
	
	def fail {
		jobCompleted
		channel.send( new WorkFailReq( jobHandle ) )
	}
	
	def exception( data: String ) {
		jobCompleted
		channel.send( new WorkExceptionReq( jobHandle, data ) )
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
 */  
class GearmanWorker( servers: String, maxOnGoingJobs: Int ) {
	val funcHandlers = new HashMap[ String, WorkFuncHandler ]
	val serverAddrs = parseAddressList( servers )
	val channels = new java.util.LinkedList[MessageChannel]
	
	// all the on-going jobs
	val jobs = new JobList
		
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
			if( channel == null ) {
				start( addr )
			} else {
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
				case JobAssign( jobHandle, funcName, data ) => handleJob( from, jobHandle, funcName, data, None )
				case JobAssignUniq( jobHandle, funcName, uid, data ) => handleJob( from, jobHandle, funcName, data, Some( uid ) )
				case Noop() => grabJob( from )
				case NoJob() => from.send( new PreSleep )																		
				case _ => from.send( new PreSleep )
			} 
		}
		
		override def handleDisconnect( from: MessageChannel ) {
			channels.synchronized { channels.remove( from ) }
			start( addr )
		}
	}
	
	private def handleJob( from: MessageChannel, jobHandle: String, funcName: String, data: String, uid: Option[String] ) {
		funcHandlers.synchronized {
			funcHandlers.get( funcName ) match {
				case Some( handler ) => 
					jobs.addJob( jobHandle, from )
					val completeCb = { handleJobCompleted( from, jobHandle ) }					
					future { handler.handle( jobHandle, funcName, data, uid, new DefWorkResponser( jobHandle, from, completeCb ) ) }
				case _ => from.send( new Error( "2", "No handler found") )
			}
		}
		
		grabJob( from )
	}
	
	private def handleJobCompleted( channel: MessageChannel, jobHandle: String ) {
		jobs.removeJob( jobHandle )
		grabJob( channel )
	}
	
	private def grabJob( channel: MessageChannel ) {
		if( maxOnGoingJobs <= 0 || jobs.size < maxOnGoingJobs ) channel.send( new GrabJob )
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
		val worker = new GearmanWorker( "127.0.0.1:3333,127.0.0.1:3334", 10 )
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
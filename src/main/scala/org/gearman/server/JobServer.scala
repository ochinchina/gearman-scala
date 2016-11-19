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
package org.gearman.server

import org.gearman.message._
import org.gearman.channel._
import akka.actor.{ActorSystem, Props}
import scala.collection.mutable.{HashMap,LinkedList,HashSet,PriorityQueue}
import java.util.{UUID, Timer, TimerTask}
import scala.util.control.Breaks.{breakable,break}
import java.util.concurrent.{Executors, ExecutorService}
import java.net.{SocketAddress, InetSocketAddress}


/**
 * receive all the jobs from the clients and dispatch them to workers. Forward
 * the job data got from the workers to the related client.
 *
 * @param executor thread pool used to handle job timeout  
 *    
 * @author Steven Ou   
 */ 
class JobServer( sockAddr: SocketAddress ) extends MessageHandler {
	private val actorSystem = ActorSystem("Gearman")
	private val jobManager = actorSystem.actorOf( Props[JobManager] )
	private val executor = Executors.newFixedThreadPool( 1 )	
	@volatile
	private var stopped = false
	
	private val serverSockChannel = AsyncSockMessageChannel.accept( sockAddr, (channel:MessageChannel) => {
				channel.setMessageHandler( this )
				channel.open
			}, Some( executor ) )
		
	/**
	 *  handle the message received from the client or worker
	 *  
	 * @param msg the received gearman message
	 * @param from the client or worker message channel	 	 	 
	 */	 	
	override def handleMessage( msg: Message, from: MessageChannel ) {
		import Message._
		
		println( "handleMessage:" + msg )
		
		msg match {
			case EchoReq(data) => from.send( EchoRes(data ) )
			case canDo: CanDo => jobManager ! (from, canDo )
			case cantDo: CantDo=>  jobManager ! ( from, cantDo )
			case canDoTimeout: CanDoTimeout => jobManager !( from, canDoTimeout )
			case resetAbilities: ResetAbilities => jobManager ! (from, resetAbilities )
			case SubmitJob( funcName, uniqId, data ) => jobManager ! new Job( funcName, UUID.randomUUID().toString, uniqId, data, None, JobPriority.Normal, false, from )
			case SubmitJobBg( funcName, uniqId, data ) => jobManager ! new Job( funcName, UUID.randomUUID().toString, uniqId, data, None, JobPriority.Normal, true, from )
			case SubmitJobHigh( funcName, uniqId, data ) => jobManager ! new Job( funcName, UUID.randomUUID().toString, uniqId, data, None, JobPriority.High, false, from )
			case SubmitJobHighBg( funcName, uniqId, data ) => jobManager ! new Job( funcName, UUID.randomUUID().toString, uniqId, data, None, JobPriority.High, true, from )
			case SubmitJobLow( funcName, uniqId, data ) => jobManager ! new Job( funcName, UUID.randomUUID().toString, uniqId, data, None, JobPriority.Low, false, from )
			case SubmitJobLowBg( funcName, uniqId, data ) => jobManager ! new Job( funcName, UUID.randomUUID().toString, uniqId, data, None, JobPriority.Low, true, from )
			case SubmitReduceJob( funcName, uniqId, reducer, data ) => jobManager ! new Job( funcName, UUID.randomUUID().toString, uniqId, data, Some(reducer), JobPriority.Low, false, from )
			case SubmitReduceJobBg( funcName, uniqId, reducer, data ) => jobManager ! new Job( funcName, UUID.randomUUID().toString, uniqId, data, Some(reducer), JobPriority.Low, true, from )
			case GrabJob() => jobManager ! ( from, GrabJob() )
			case GrabJobUniq() => jobManager ! ( from, GrabJobUniq() )
			case grabJobAll: GrabJobAll => jobManager ! ( from, grabJobAll )
			case getStatus: GetStatus => jobManager ! (from, getStatus )
			case getStatusUnique: GetStatusUnique => jobManager ! (from, getStatusUnique )
			case workDataReq: WorkDataReq => jobManager ! ( from, workDataReq )
			case workWarningReq: WorkWarningReq => jobManager ! (from, workWarningReq ) 
			case workStatusReq:WorkStatusReq=> jobManager ! (from, workStatusReq )
			case workFailReq:WorkFailReq => jobManager ! (from, workFailReq )
			case workExceptionReq: WorkExceptionReq => jobManager ! (from, workExceptionReq )
			case workCompleteReq:WorkCompleteReq => jobManager ! (from, workCompleteReq )
			case preSleep: PreSleep => jobManager !( from, preSleep )
			case setClientId: SetClientId => jobManager ! (from, setClientId)//workers.setId( from, id )
			case adminRequest: AdminRequest => jobManager ! (from, adminRequest)//handleAdminRequest( from, command, args )
			case _ =>     
		}
	}
	
	def handleDisconnect( from: MessageChannel ) = {
	  jobManager ! (from, "connectionLost" )
	}
/*	
	private def handleAdminRequest( from: MessageChannel, command: String, args: List[ String ] ) {
		var respLines = List[String]()					
		command match {
			case "workers" =>
				
				workers.getWorkers.foreach( worker => {
					val sb = new StringBuilder
					sb.append( "-1 ").append( worker.getAddress ).append( ' ')
					sb.append( workers.getId( worker ).getOrElse( "-") )
					sb.append( ":")
					workers.getWorkerFuncs( worker ).getOrElse( scala.collection.Set[String]() ).foreach( funcName=> {
						sb.append( ' ' ).append( funcName )
					})
					
					respLines = respLines :+ sb.toString				
				})
				
				respLines = respLines :+ "."
			
			case "maxqueue" =>
				args.size match {
					case 0 =>
					case 1 => jobs.setQueueSize( args(0), -1 )
					case _ => jobs.setQueueSize( args(0), args(1).toInt )
				}
				respLines = respLines :+ "OK"	
			case "status" =>
				var funcs = Set[ String ]()
				val allJobs = jobs.getAllJobs
				
				allJobs.foreach { job => funcs = funcs + job.funcName }
				
				funcs = funcs ++ workers.getAllFunctions
				
				funcs.foreach { funcName =>
					val sb = new StringBuilder
					sb.append( funcName ).append( '\t')
					val funcJobs = allJobs.filter( job => job.funcName == funcName )
					val funcRunningJobs = funcJobs.filter( job => job.processing != null )
					sb.append( funcJobs.size ).append( '\t')
					sb.append( funcRunningJobs.size ).append( '\t')
					sb.append( workers.getFuncWorkers( funcName ).getOrElse( new HashMap[MessageChannel, Int] ).size )
					respLines = respLines :+ sb.toString					
				}
				respLines = respLines :+ "."				
			case "shutdown" =>
				val graceful = args.size > 0 && args(0) == "graceful"
				shutdown( graceful, System.exit( 0 ) ) 
			case "version" => respLines = respLines :+ "1.0"
			case _ =>                                       
		}
		
		if( respLines.size > 0 ) from.send( AdminResponse( respLines ) )
	} */
}

object JobServer {
	def apply( sockAddr: SocketAddress) = new JobServer( sockAddr )
	
	def apply( listeningAddr: String, port: Int ) = new JobServer( new InetSocketAddress( listeningAddr, port ) )
	
	def apply( port: Int ) = new JobServer( new InetSocketAddress( port ) )		
}

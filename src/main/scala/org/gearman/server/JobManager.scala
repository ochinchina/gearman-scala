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
import akka.actor.{Actor, ActorRef}
import scala.collection.mutable.{HashMap,Set,MultiMap,PriorityQueue}
import scala.collection.immutable.List
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * manages all the received jobs from the client. The job will be divided into 
 * different categories: pending jobs, running jobs.  
 *
 */
class JobManager extends Actor {

	private val jobs = new JobList()
	private val workers = new WorkerManager()

	def receive = {
		case job: Job => handleJob( job )
		case (client:MessageChannel, CanDo(funcName)) => workers.addFunc( client, funcName )
		case (client:MessageChannel, CantDo(funcName)) => workers.removeFunc(client, funcName)
		case (client:MessageChannel, CanDoTimeout(funcName, timeout ) ) => workers.addFunc( client, funcName, timeout )
		case (client:MessageChannel, ResetAbilities() ) => workers.remove( client )
		case (client:MessageChannel, GrabJob() ) => handleGrabJob(client, false, false )
		case (client:MessageChannel, GrabJobUniq() ) => handleGrabJob( client, true, false )
		case (client:MessageChannel, GrabJobAll() ) => handleGrabJob( client, true, true)
		case (client:MessageChannel, PreSleep() ) => workers.sleep( client )
		case (client:MessageChannel, GetStatus( jobHandle ) ) => handleGetStatus( client, jobHandle )
		case (client:MessageChannel, GetStatusUnique( uniqId ) ) => handleGetStatusUnique( client, uniqId )
		case (client:MessageChannel, WorkDataReq( jobHandle, data ) ) => sendWorkData( client, jobHandle, new WorkDataRes(jobHandle, data) )
		case (client:MessageChannel, WorkWarningReq( jobHandle, data ) ) => sendWorkData( client, jobHandle, new WorkWarningRes( jobHandle, data ) )
		case (client:MessageChannel, WorkFailReq( jobHandle ) ) => sendWorkData( client, jobHandle, new WorkFailRes( jobHandle ), true )
		case (client:MessageChannel, WorkExceptionReq( jobHandle,data ) ) => sendWorkData( client, jobHandle, new WorkExceptionRes( jobHandle,data ), true )
		case (client:MessageChannel, WorkCompleteReq( jobHandle,data ) ) => 
		  sendWorkData( client, jobHandle, new WorkCompleteRes( jobHandle,data ), true )
		case (client:MessageChannel, SetClientId( clientId ) ) => workers.setId( client, clientId )
		case (client:MessageChannel, AdminRequest( command, args ) ) =>
		case ("timeout", job:Job) => 
		  if( jobs.remove( job ) ) job.from.send( WorkFailRes( job.jobHandle) )
		case (client:MessageChannel, "connectionLost" ) => handleConnectionLost( client )
		case _ =>
	}

  private def handleJob( job: Job ) {
    println( "handleJob:" + job )
    if( jobs.add( job ) ) {
      job.from.send( JobCreated( job.jobHandle ) )
      workers.wakeup( job.funcName ).foreach{ client => client.send( Noop() ) }
    } else {
      job.from.close
    }
  }
  
  private def handleConnectionLost( client: MessageChannel ) = {
    var clientJobs = List[Job]()
    
    //remove all the not background job  from client
    jobs.foreach{ job => if(job.from == client && !job.background ) clientJobs = (job +: clientJobs) }
    
    clientJobs.foreach{ job => jobs.remove( job ) }
  }

  private def handleGrabJob( client: MessageChannel, uniq: Boolean, reduce: Boolean ) = {
	println( "handleGrabJob,client=" + client )
    workers.getFuncs(client) match {
      case funcs: List[WorkerFuncInfo] =>
        jobs.find( jb => jb.processing == null && funcs.find( _.funcName == jb.funcName ) != None ) match {
          case Some(job) =>
            if (reduce && job.reducer != None ) {
              client.send ( JobAssignAll (job.jobHandle, job.funcName, job.uniqId, job.reducer.get, job.data) )
            } else if (uniq) {
              client.send (JobAssignUniq (job.jobHandle, job.funcName, job.uniqId, job.data) )
            } else {
              client.send ( JobAssign (job.jobHandle, job.funcName, job.data) )
            }
            job.processing = client
            val timeout = funcs.find( _.funcName == job.funcName ).get.timeout
            if( !job.background && timeout > 0 ) context.system.scheduler.scheduleOnce( timeout seconds, self, ("timeout", job ) )
	          
          case _ => client.send( NoJob() )
        }
      case _ => client.send( NoJob() )   
    }
  }

  
  
  private def handleGetStatus( client: MessageChannel, jobHandle: String ) = {
  	jobs.find( _.jobHandle == jobHandle ) match {
  		case Some( job ) =>
  			client.send( StatusRes( jobHandle, job.numerator != 0 && job.denominator != 0, job.processing != null, job.numerator, job.denominator ) )
  		case _ =>
  			client.send( StatusRes( jobHandle, false, false, 0, 0) )
	}
  }    
  
  private def handleGetStatusUnique( client: MessageChannel, uniqId: String ) = {
    
    val waitingClients = Set[MessageChannel]()
    jobs.foreach{ waitingClients += _.from }
    
  	jobs.find( _.uniqId == uniqId ) match {
  		case Some( job ) =>
  			client.send( StatusResUnique(job.jobHandle, job.numerator != 0 && job.denominator != 0, job.processing != null, job.numerator, job.denominator,  waitingClients.size ) )
  		case _ =>
  			client.send( StatusResUnique( "-", false, false, 0, 0, waitingClients.size ) )
	}
  }  
  
  private def sendWorkData( client: MessageChannel, 
  		jobHandle: String,
  		msg: Message,
  		complete: Boolean = false ) { 		
  		  jobs.find( _.jobHandle == jobHandle ) match {
  		      case Some( job ) =>
  		      if( client == job.processing ) {
  		        if( !job.background ) {
  		          job.from.send( msg )
  		        }
  		        if( complete ) {
  		          jobs.remove(  job )
  		        }
  		      }
  		      case _ =>
  		  }
  }

    
  private def handleAdminRequest( from: MessageChannel, command: String, args: List[ String ] ) {
		var respLines = List[String]()					
		command match {
			case "workers" =>				
				workers.foreachWorker{ case (worker, funcs) => {
						val sb = new StringBuilder
						sb.append( "-1 ").append( worker.getAddress ).append( ' ')
						sb.append( workers.getId( worker ).getOrElse( "-") )
						sb.append( ":")
						funcs.foreach{ funcName => sb.append( ' ' ).append( funcName ) }
						respLines = respLines :+ sb.toString				
					}
				}
				
				respLines = respLines :+ "."
			
			case "maxqueue" =>
				args.size match {
					case 2 => 
						jobs.setQueueSize( args(0), args(1).toInt )
					case 3 => 
						jobs.setQueueSize( args(0), JobPriority.High, args(1).toInt )
						jobs.setQueueSize( args(0), JobPriority.Normal, args(2).toInt )
						jobs.setQueueSize( args(0), JobPriority.Low, args(3).toInt )
				}
				respLines = respLines :+ "OK"	
			case "status" =>
				workers.foreachFunc {
					case (funcName, clients ) =>
						val sb = new StringBuilder
						sb.append( funcName ).append( '\t')
						val totalJobs = jobs.map{ job => if(job.funcName == funcName) 1 else 0 }.sum
						val runingJobs = jobs.map( job => if( job.funcName == funcName && job.processing != null ) 1 else 0).sum
						val availWorkers = clients.size
						sb.append( totalJobs ).append( '\t')
						sb.append( runingJobs ).append( '\t')
						sb.append( availWorkers )
						respLines = respLines :+ sb.toString
					case _ =>				
				}
				respLines = respLines :+ "."
												
			case "shutdown" =>
				val graceful = args.size > 0 && args(0) == "graceful"
				//shutdown( graceful, System.exit( 0 ) )
			case "version" => respLines = respLines :+ "1.1"
			case _ =>                                       
		}
		
		if( respLines.size > 0 ) from.send( AdminResponse( respLines ) )
	} 
  
  
    
}



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
import scala.collection.mutable.{HashMap,LinkedList,HashSet,PriorityQueue}
import java.util.{UUID, Timer, TimerTask}
import scala.util.control.Breaks.{breakable,break}
import java.util.concurrent.{ExecutorService}



object JobPriority extends Enumeration {
	type JobPriority = Value
	val Low, Normal, High = Value
}



case class Job( funcName: String, 
			jobHandle: String, 
			uniqId: String, 
			data: String, 
			priority: JobPriority.JobPriority,
			background: Boolean, 
			from: MessageChannel,
			var processing: MessageChannel,
			var numerator: Int = 0,
			var denominator: Int = 0  ) {
}

class JobPriorityOrdering extends Ordering[ Job ] {
	def compare(job1: Job, job2: Job ) = {
		job1.priority match {
			case JobPriority.Low =>
				job2.priority match {
					case JobPriority.Low => 0
					case JobPriority.Normal => -1
					case JobPriority.High => -1
				}
			case JobPriority.Normal =>
				job2.priority match {
					case JobPriority.Low => 1
					case JobPriority.Normal => 0
					case JobPriority.High => -1
				}
			case JobPriority.High =>
				job2.priority match {
					case JobPriority.Low => 1
					case JobPriority.Normal => 1
					case JobPriority.High => 0
				}
		}
	}
}


class WorkerManager() {
	/* mapping between the function name and the worker message channel*/
	private val funcWorkers = new HashMap[String, HashMap[ MessageChannel, Int ] ]
	/*mapping between the worker ( message channel ) and its functions*/ 
	private val workerFuncs = new HashMap[MessageChannel, HashMap[ String, Int ] ]
	
	private val presleepWorkers = new HashSet[ MessageChannel ]
	
	private val workerIds = new HashMap[ MessageChannel, String ]

	def addWorkerFunc( funcName: String, channel: MessageChannel, timeout: Int = 0 ) {
		if( !funcWorkers.contains( funcName ) ) funcWorkers += funcName -> new HashMap[MessageChannel, Int ]
		funcWorkers( funcName ) += channel -> timeout
		if( !workerFuncs.contains( channel ) ) workerFuncs += channel -> new HashMap[ String, Int ]
		workerFuncs(channel) += funcName -> timeout
	}
	
	def removeWorkerFunc( funcName: String, channel: MessageChannel ) {
		if( funcWorkers.contains( funcName ) ) {
			funcWorkers( funcName ) -= channel
			if( funcWorkers( funcName ).isEmpty )  funcWorkers -=  funcName
		}
		
		if( workerFuncs.contains( channel ) ) {
			workerFuncs( channel ) -= funcName
			if( workerFuncs( channel ).isEmpty ) workerFuncs -= channel
		}
	}
	
	def resetWorkerFunc( channel: MessageChannel ) {
		workerFuncs.get( channel ) match {
			case Some( funcs ) =>
				funcs.foreach{ p =>
					funcWorkers( p._1 ) -= channel
					if( funcWorkers( p._1 ).isEmpty ) funcWorkers -= p._1
				}
				workerFuncs -= channel
			case _ =>
		}
	}
	
	def getWorkerFuncs( channel: MessageChannel ): Option[ scala.collection.Set[ String ] ] = {
		val funcs = workerFuncs.get( channel )
		if( funcs.isEmpty ) None else Some( funcs.get.keySet ) 
	}
	
	/**
	 *  get all the registered workers
	 *  
	 * @return all the workers ( presents in a MessageChannel Set )	 	  
	 */	 	
	def getWorkers = workerFuncs.keySet
	
	def getAllFunctions = funcWorkers.keySet  
	
	/**
	 *  get all the workers that have function <code>funcName</code>
	 *  
	 * @return a HashMap[ MessageChannel, Int], Int is the timeout of worker	 	 
	 */	 	
	def getFuncWorkers( funcName: String ): Option[ HashMap[MessageChannel, Int] ] =  funcWorkers.get( funcName )
	
	def getTimeout( funcName: String, channel: MessageChannel ): Int = {
		funcWorkers.get( funcName ) match {
			case Some( channelTimeout ) =>  channelTimeout.get( channel ).getOrElse( 0 )
			case _ => 0
		}
	}

	def setPreSleep( channel: MessageChannel, presleep: Boolean ) {
		if( presleep ) presleepWorkers += channel else presleepWorkers -= channel		
	}
	
	def isPreSleep( channel: MessageChannel ): Boolean = presleepWorkers.contains( channel )
	
	def setId( channel: MessageChannel, id: String ) {
		workerIds += channel -> id
	}
	
	def getId( channel: MessageChannel ): Option[ String ] = workerIds.get( channel )		
}

class JobManager() {
	private val jobQueueSize = new HashMap[String, Int ] 
	/*all the pending jobs, mapping between [funcName, jobs ]*/	
	private val pendingJobs = new HashMap[String, PriorityQueue[Job] ]
	/*the submitted jobs by client, mapping between [client, [jobHandle, job ] ]*/ 
	private val submittedJobs = new HashMap[ MessageChannel, HashMap[ String, Job ] ]
	/*the processing jobs by client, mapping between [client, [jobHandle, job ] ]*/
	private val processingJobs = new HashMap[ MessageChannel, HashMap[ String, Job ] ]
	
    /**
     *  submit a new job
     *  
     * @param job the new job submitted	      
     */	     
	def submitJob( job: Job ) : Boolean = {
		
		if( isQueueFull( job.funcName ) ) {
			return false
		} 
		if( !pendingJobs.contains( job.funcName ) ) {
			pendingJobs += job.funcName -> new PriorityQueue()( new JobPriorityOrdering )
		}
		
		pendingJobs( job.funcName ).enqueue( job )
		
		if( !submittedJobs.contains( job.from ) ) {
			submittedJobs += job.from -> new HashMap[ String, Job ]
		}
		
		submittedJobs( job.from ) += job.jobHandle -> job
		true 
	}
	
	def getJob( from: MessageChannel, jobHandle: String ): Option[ Job ] = {
		submittedJobs.get( from ) match {
			case Some( clientJobs ) => clientJobs.get( jobHandle )
			case _ => None
		}
	}
	
	def getProcessingJob( channel: MessageChannel, jobHandle: String ): Option[ Job ] = {
		processingJobs.get( channel ) match {
			case Some( jobs ) => 
				val job = jobs.get( jobHandle )
				if( !job.isEmpty && job.get.processing == channel ) job else None
			case _ => None
		}
	}
	
	def channelDisconnected( channel: MessageChannel ) {
		if( !channel.isConnected ) {			
			submittedJobs.get( channel ) match {
				case Some( jobs ) =>
					jobs.filterNot{ p => p._2.background }.foreach {
						submittedJobs( channel ) -= _._1 
					}
				case _ =>
			}
			
			processingJobs.remove( channel ) match {
				case Some( jobs ) =>
					jobs.foreach{
						case (jobHandle, job ) =>
							job.processing = null
							job.numerator = 0
							job.denominator = 0
							submitJob( job )
						case _ =>
					}
				case _ =>
			}
		}
	}
	
	def removeJob( job: Job ) {
		if( submittedJobs.contains( job.from ) ) {
			submittedJobs( job.from ) -= job.jobHandle
		}
		
		if( processingJobs.contains( job.processing ) ) {
			processingJobs( job.processing ) -= job.jobHandle
		}
	}
	
	
	def getJobCount( funcName: String ) = if( pendingJobs.contains( funcName ) )  pendingJobs( funcName ).size else 0
	
	def takeJob( funcName: String, channel: MessageChannel ): Option[ Job ] = {
		var job: Option[ Job ] = None 
		try {
			job = Some( pendingJobs( funcName ).dequeue )
			job.get.processing = channel
			
			if( pendingJobs( funcName ).isEmpty ) pendingJobs -= funcName
			
			//add to processing job map
			if( !processingJobs.contains( channel ) ) {
				processingJobs += channel -> new HashMap[ String, Job ]
			}
			
			processingJobs( channel ) += job.get.jobHandle -> job.get
			
		}catch {
			case _:Throwable =>
		}

		job
	}
	
	def takeJob( funcs: scala.collection.Set[ String ], channel: MessageChannel ) :Option[ Job ] = {
		var job: Option[ Job ] = None
		
		funcs.foreach{
			( funcName: String ) => if( job.isEmpty ) job = takeJob( funcName, channel )
		}
			
		job
	}
	
	def getAllJobs: List[ Job ] = {
		var allJobs = List[Job]()
		
		submittedJobs.foreach{ channelJobs => 
			channelJobs._2.foreach{ funcJobs => 
				allJobs = allJobs :+ funcJobs._2 
			} 
		}
		
		allJobs 
	}
	
	def getQueueSize( funcName: String ) : Int = if( jobQueueSize.contains( funcName ) ) jobQueueSize(funcName) else -1
	
	def setQueueSize( funcName: String, size: Int ) {
		jobQueueSize += funcName -> size
	}
	
	private def isQueueFull( funcName: String ) : Boolean = pendingJobs.contains( funcName ) && pendingJobs( funcName ).size < getQueueSize( funcName )
		
}

class JobServer( executor: ExecutorService ) extends MessageHandler {
	private val workers = new WorkerManager
	private val jobs = new JobManager
	private val timer = new Timer
	
	
	def handleMessage( msg: Message, from: MessageChannel ) {
		import Message._
		
		msg match {
			case EchoReq(data) => handleEchoReq( from, data )
			case CanDo( funcName ) => handleCanDo( from, funcName )
			case CantDo( funcName ) =>  handleCantDo( from, funcName )
			case CanDoTimeout( funcName, timeout ) => handleCanDoTimeout( from, funcName, timeout )
			case ResetAbilities() => handleResetAbilities( from )
			case SubmitJob( funcName, uniqId, data ) =>
				val job = new Job( funcName, UUID.randomUUID.toString, uniqId, data, JobPriority.Normal, false, from, null )  
				handleSubmitJob( from, job )
				from.send( new JobCreated( job.jobHandle ) )
			case SubmitJobBg( funcName, uniqId, data ) =>
				val job = new Job( funcName, UUID.randomUUID.toString, uniqId, data, JobPriority.Normal, true, from, null )  
				handleSubmitJob( from, job )
				from.send( new JobCreated( job.jobHandle ) )
			case SubmitJobHigh( funcName, uniqId, data ) =>
				val job = new Job( funcName, UUID.randomUUID.toString, uniqId, data, JobPriority.High, false, from, null )
				handleSubmitJob( from, job )
				from.send( new JobCreated( job.jobHandle ) )
			case SubmitJobHighBg( funcName, uniqId, data ) =>
				val job = new Job( funcName, UUID.randomUUID.toString, uniqId, data, JobPriority.High, true, from, null )
				handleSubmitJob( from, job )
				from.send( new JobCreated( job.jobHandle ) )
			case SubmitJobLow( funcName, uniqId, data ) =>
				val job = new Job( funcName, UUID.randomUUID.toString, uniqId, data, JobPriority.Low, false, from, null )
				handleSubmitJob( from, job )
				from.send( new JobCreated( job.jobHandle ) )
			case SubmitJobLowBg( funcName, uniqId, data ) =>
				val job = new Job( funcName, UUID.randomUUID.toString, uniqId, data, JobPriority.Low, true, from, null )
				handleSubmitJob( from, job )
				from.send( new JobCreated( job.jobHandle ) )
			case GrabJob() => handleGrabJob( from, false )
			case GrabJobUniq() => handleGrabJob( from, true )
			case GetStatus( jobHandle ) => handleGetStatus( from, jobHandle )
			case WorkDataReq( jobHandle, data ) => handleWorkDataReq( from, jobHandle, data )
			case WorkWarningReq( jobHandle, data ) => handleWorkWarningReq( from, jobHandle, data )
			case WorkStatusReq( jobHandle, numerator: Int, denominator: Int ) => handleWorkStatusReq( from, jobHandle, numerator, denominator )
			case WorkFailReq( jobHandle ) => handleWorkFailReq( from, jobHandle )
			case WorkExceptionReq( jobHandle, data ) => handleWorkExceptionReq( from, jobHandle, data )
			case WorkCompleteReq( jobHandle: String, data: String ) => handleWorkCompleteReq( from, jobHandle, data )
			case PreSleep() =>
				workers.setPreSleep( from, true )
				if( getPendingJobCountForWorker( from ) > 0 ) from.send( new Noop )
				
			case SetClientId( id ) => workers.setId( from, id )
			case AdminRequest( command, args ) => handleAdminRequest( from, command, args )
			case _ =>     
		}
	}
	
	def handleDisconnect( from: MessageChannel ) {
		jobs.channelDisconnected( from )
		workers.resetWorkerFunc( from )
	}
	
	private def getPendingJobCountForWorker( from: MessageChannel ) = {
		var pending = 0
		
		workers.getWorkerFuncs( from ) match {
			case Some( funcs ) => funcs.foreach{ funcName => pending += jobs.getJobCount( funcName ) }
			case _ =>
		}
		pending 
	}
	
	private def handleEchoReq( from: MessageChannel, data: String ) {
		from.send( new EchoRes( data ) )
	}
	
	private def handleCanDo( from: MessageChannel, funcName: String ) {
		workers.addWorkerFunc( funcName, from  )
	}
	
	private def handleCantDo( from: MessageChannel, funcName: String ) {
		workers.removeWorkerFunc( funcName, from )
	}
	
	private def handleCanDoTimeout( from: MessageChannel, funcName: String, timeout: Int ) {
		workers.addWorkerFunc( funcName, from, timeout )
	}
	
	private def handleResetAbilities( from: MessageChannel ) {
		workers.resetWorkerFunc( from )
	}
	
	private def handleSubmitJob( from: MessageChannel, job: Job )  {
		jobs.submitJob( job )
		workers.getFuncWorkers( job.funcName ) match {
			case Some( channels ) => 
				channels.foreach( p =>
					if( workers.isPreSleep( p._1 ) ) {
						p._1.send( new Noop )
					}  
				)
			case _ =>
		}
		
	}
	
	private def handleGrabJob( from: MessageChannel, uniq: Boolean ) {				
		workers.getWorkerFuncs( from ) match {
			case Some( funcs ) =>
				jobs.takeJob( funcs, from ) match {
					case Some( job ) =>
						workers.setPreSleep( from, false ) 
						if( uniq )
							from.send( new JobAssignUniq( job.jobHandle, job.funcName, job.uniqId, job.data ) )
						else from.send( new JobAssign( job.jobHandle, job.funcName, job.data ) )
						val timeout = workers.getTimeout( job.funcName, from )
						if( timeout > 0 ) timer.schedule( new TimerTask { def run { handleTimeout( from, job.jobHandle ) } }, timeout * 1000 )
					case _ =>  from.send( new NoJob() )
				}
				
			case _ => from.send( new NoJob() )
		}
	}
	
	private def handleGetStatus( from: MessageChannel, jobHandle: String ) {
		jobs.getJob( from, jobHandle ) match {
			case Some( job ) => from.send( new StatusRes( jobHandle, false, if( job.processing != null ) true else false, job.numerator, job.denominator ) )
			case _ => from.send( new StatusRes( jobHandle, true, false, 0, 0 ) )
		}
	}
	
	private def handleWorkDataReq( from: MessageChannel, jobHandle: String, data: String ) {
		jobs.getProcessingJob( from, jobHandle ) match {
			case Some( job ) => if( !job.background ) job.from.send( new WorkDataRes( jobHandle, data ) )
			case _ =>
		}
	}
	
	private def handleWorkWarningReq( from: MessageChannel, jobHandle: String, data: String ) {
		jobs.getProcessingJob( from, jobHandle ) match {
			case Some( job ) => if( !job.background ) job.from.send( new WorkWarningRes( jobHandle, data ) )
			case _ =>
		}
	}
	
	private def handleWorkStatusReq( from: MessageChannel, jobHandle: String, numerator: Int, denominator: Int ) {
		jobs.getProcessingJob( from, jobHandle ) match {
			case Some( job ) =>
				job.numerator = numerator
				job.denominator = denominator 
				if( !job.background ) job.from.send( new WorkStatusRes( jobHandle, numerator, denominator ) )
			case _ =>
		}
	}
	
	private def handleWorkFailReq( from: MessageChannel, jobHandle: String ) {
		jobs.getProcessingJob( from, jobHandle ) match {
			case Some( job ) => 
				if( !job.background ) job.from.send( new WorkFailRes( jobHandle ) )
				jobs.removeJob( job )
			case _ =>
		}
	}
	
	private def handleWorkExceptionReq( from: MessageChannel, jobHandle: String, data: String ) {
		jobs.getProcessingJob( from, jobHandle ) match {
			case Some( job ) => 
				if( !job.background ) job.from.send( new WorkExceptionRes( jobHandle, data ) )
				jobs.removeJob( job )
			case _ =>
		}
	}
	
	private def handleWorkCompleteReq( from: MessageChannel, jobHandle: String, data: String ) {
		jobs.getProcessingJob( from, jobHandle ) match {
			case Some( job ) => 
				if( !job.background ) job.from.send( new WorkCompleteRes( jobHandle, data ) )
				jobs.removeJob( job )
			case _ =>
		}
	}
	
	private def handleTimeout( processingChannel: MessageChannel, jobHandle: String ) {
		executor.submit( new Runnable { def run {
			jobs.getProcessingJob( processingChannel, jobHandle ) match {
				case Some( job ) => 
					if( !job.background ) job.from.send( new WorkFailRes( jobHandle ) )
					jobs.removeJob( job )
				case _ =>
			}
		}})
	}
	
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
			case "shutdown" => println( "shutdown")
			case "version" => respLines = respLines :+ "1.0"
			case _ =>                                       
		}
		
		if( respLines.size > 0 ) from.send( new AdminResponse( respLines ) )
	} 
}

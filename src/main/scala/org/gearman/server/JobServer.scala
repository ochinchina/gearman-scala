package org.gearman.server

import org.gearman.message._
import org.gearman.channel._
import scala.collection.mutable.{HashMap,LinkedList,HashSet,PriorityQueue}
import java.util.{UUID}
import scala.util.control.Breaks.{breakable,break}



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

	def addWorkerFunc( funcName: String, channel: MessageChannel, timeout: Int = -1 ) {
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
	
	def getFuncWorkers( funcName: String ) =  funcWorkers.get( funcName )

	def setWorkerPreSleep( channel: MessageChannel, presleep: Boolean ) {
		if( presleep ) presleepWorkers += channel else presleepWorkers -= channel		
	}
	
	def isWorkerPreSleep( channel: MessageChannel ) = presleepWorkers.contains( channel )
}

class JobManager() {
	/*all the pending jobs, mapping between [funcName, jobs ]*/	
	private val pendingJobs = new HashMap[String, PriorityQueue[Job] ]
	/*the submitted jobs by client, mapping between [client, [jobHandle, job ] ]*/ 
	private val submittedJobs = new HashMap[ MessageChannel, HashMap[ String, Job ] ]
	/*the processing jobs by client, mapping between [client, [jobHandle, job ] ]*/
	private val processingJobs = new HashMap[ MessageChannel, HashMap[ String, Job ] ]
	
    /**
     *  add a new job
     */	     
	def submitJob( job: Job ) {
		if( !pendingJobs.contains( job.funcName ) ) {
			pendingJobs += job.funcName -> new PriorityQueue()( new JobPriorityOrdering )
		}
		
		pendingJobs( job.funcName ).enqueue( job )
		
		if( !submittedJobs.contains( job.from ) ) {
			submittedJobs += job.from -> new HashMap[ String, Job ]
		}
		
		submittedJobs( job.from ) += job.jobHandle -> job 
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
		
}

class JobServer extends MessageHandler {
	private val workers = new WorkerManager
	private val jobs = new JobManager
	
	
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
			case GrabJob() => handleGrabJob( from )
			case GetStatus( jobHandle ) => handleGetStatus( from, jobHandle )
			case WorkDataReq( jobHandle, data ) => handleWorkDataReq( from, jobHandle, data )
			case WorkWarningReq( jobHandle, data ) => handleWorkWarningReq( from, jobHandle, data )
			case WorkStatusReq( jobHandle, numerator: Int, denominator: Int ) => handleWorkStatusReq( from, jobHandle, numerator, denominator )
			case WorkFailReq( jobHandle ) => handleWorkFailReq( from, jobHandle )
			case WorkExceptionReq( jobHandle, data ) => handleWorkExceptionReq( from, jobHandle, data )
			case WorkCompleteReq( jobHandle: String, data: String ) => handleWorkCompleteReq( from, jobHandle, data )
			case _ =>     
		}
	}
	
	def handleDisconnect( from: MessageChannel ) {
		jobs.channelDisconnected( from )
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
	}
	
	private def handleGrabJob( from: MessageChannel ) {				
		workers.getWorkerFuncs( from ) match {
			case Some( funcs ) =>
				jobs.takeJob( funcs, from ) match {
					case Some( job ) => from.send( new JobAssign( job.jobHandle, job.funcName,job.data ) )
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
}

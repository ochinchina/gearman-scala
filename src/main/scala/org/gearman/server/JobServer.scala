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
import java.util.concurrent.{Executors, ExecutorService}
import java.net.{SocketAddress, InetSocketAddress}


/**
 * presents a received job from the client and also job status will be saved
 * to the job object
 * 
 * @param funcName function name of job
 * @param jobHandle the assigned unique job handle by the job server
 * @param uniqId the unique job identifier assigned by client
 * @param data the job data from client
 * @param priority the job priority(depends on the gearman message)
 * @param background true if the job is a background job
 * @param from the message channel(presents a client) which this job received from
 * @param processing the processing message channel(presents a worker)
 * @param numerator the worker reported completed percentage of numerator
 * @param denominator the worker reported completed percentage of denominator              
 */ 
private case class Job( funcName: String, 
			jobHandle: String, 
			uniqId: String, 
			data: String, 
			priority: JobPriority.JobPriority,
			background: Boolean, 
			from: MessageChannel,
			var processing: MessageChannel = null,
			var numerator: Int = 0,
			var denominator: Int = 0  ) {
}

/**
 * implement the Ordering required by scala PriorityQueue. It compares the order
 * of two jobs, high priority have higher order 
 */ 
private class JobPriorityOrdering extends Ordering[ Job ] {
	/**
	 * compare two jobs with their job priority.
	 * 
	 * @return -1 if job2 has higher priority than job1
	 * 	 0 if two jobs have some priority
	 * 	 1 if job1 has higher priority than job2	 
	 * 	 	 	 
	 */	 	
	override def compare(job1: Job, job2: Job ) = {
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


/**
 * manage all the registered workers. After connecting to the gearman server, the
 * worker will send CAN_DO or CAN_DO_TIMEOUT to the gearman server. Any peer can
 * become a worker if it sends CAN_DO/CAN_DO_TIMEOUT to gearman server   
 *
 */  
private class WorkerManager() {
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
	
	/**
	 *  get all the functions
	 *  
	 * @param all the registered functions	 	 
	 */	 	
	def getAllFunctions = funcWorkers.keySet  
	
	/**
	 *  get all the workers that have function <code>funcName</code>
	 *  
	 * @return a HashMap[ MessageChannel, Int], Int is the timeout of worker	 	 
	 */	 	
	def getFuncWorkers( funcName: String ): Option[ HashMap[MessageChannel, Int] ] =  funcWorkers.get( funcName )
	
	/**
	 * get the timeout for function on the worker
	 * 
	 * @param funcName the function name
	 * @param channel the worker message channel
	 * 
	 * @return the timeout, if > 0, the timeout is set, <=0, no timeout	 	 	 	 	 
	 */	 	
	def getTimeout( funcName: String, channel: MessageChannel ): Int = {
		funcWorkers.get( funcName ) match {
			case Some( channelTimeout ) =>  channelTimeout.get( channel ).getOrElse( 0 )
			case _ => 0
		}
	}

	/**
	 * change the worker channel to presleep state
	 * 
	 * @param channel the worker channel
	 * @param presleep true if the worker enters preSleep state, false the worker
	 * exits the preSleep state	 	 	 	 
	 */	 	
	def setPreSleep( channel: MessageChannel, presleep: Boolean ) {
		if( presleep ) presleepWorkers += channel else presleepWorkers -= channel		
	}

	/**
	 * check if the worker channel is in presleep state or not
	 * 
	 * @param channel the worker channel
	 * 
	 * @return true the worker channel is in preSleep	 	 	 	 
	 */	 		
	def isPreSleep( channel: MessageChannel ): Boolean = presleepWorkers.contains( channel )
	
	/**
	 * set the worker Client ID
	 * 
	 * @param channel the worker channel
	 * @param the id set from the worker	  	 	 
	 */	 	
	def setId( channel: MessageChannel, id: String ) {
		workerIds += channel -> id
	}

	/**
	 * get the worker client ID
	 * 
	 * @param channel the worker channel
	 * @return None no ID is set, non None the ID set by the worker	 	 	 
	 */	 		
	def getId( channel: MessageChannel ): Option[ String ] = workerIds.get( channel )
	
	/**
	 *  get all the worker channels
	 */	 	
	def getAllWorkerChannels = {
		var channels = List[ MessageChannel ]()
		
		workerFuncs.foreach{ case (workerChannel, _) => 
			channels = workerChannel :: channels 
		}
		channels 
	}		
}

/**
 * manages all the received jobs from the client. The job will be divided into 
 * different categories: pending jobs, running jobs.  
 *
 */  
private class JobManager {
	/*user set queue size for function, mapping between [funcName, queueSize]*/
	private val jobQueueSize = new HashMap[String, Int ] 
	/*all the pending jobs, mapping between [funcName, jobs ]*/	
	private val pendingJobs = new HashMap[String, PriorityQueue[Job] ]
	/*the submitted jobs by client, mapping between [client, [jobHandle, job ] ]*/ 
	private val submittedJobs = new HashMap[ MessageChannel, HashMap[ String, Job ] ]
	/*the processing jobs by client, mapping between [client, [jobHandle, job ] ]*/
	private val processingJobs = new HashMap[ MessageChannel, HashMap[ String, Job ] ]
	
    /**
     *  submit a new job to the manager
     *  
     * @param job the new job submitted
     * 
     * @return true if the job is submitted successfully, false if the job is failed
     * to submit ( the job queue size is full for the function )	 	 	 	      
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
	
	/**
	 *  get the submitted and not finished job by the client channel and job handle
	 *  
	 * @param from the client channel
	 * @param jobHandle the job handle assigned by server
	 * 
	 * @return a valid job if found otherwise a None is returned	 	 	 	 	   
	 */	 	
	def getJob( from: MessageChannel, jobHandle: String ): Option[ Job ] = {
		submittedJobs.get( from ) match {
			case Some( clientJobs ) => clientJobs.get( jobHandle )
			case _ => None
		}
	}
	
	/**
	 * get the processing job by the worker channel and the job handle
	 * 
	 * @param channel the worker channel
	 * @param jobHandle the job handle assigned by server	 	 	 
	 *
	 * @return a valid job if the job is processing by the worker channel otherwise
	 * a None is returned	 	 
	 */	 	 	
	def getProcessingJob( channel: MessageChannel, jobHandle: String ): Option[ Job ] = {
		processingJobs.get( channel ) match {
			case Some( jobs ) => 
				val job = jobs.get( jobHandle )
				if( !job.isEmpty && job.get.processing == channel ) job else None
			case _ => None
		}
	}
	
	/**
	 * a channel ( client channel or worker channel ) connection is lost.
	 * If a client channel is connection lost, all non-background job will be
	 * cleared.
	 * If a worker channel is connection lost, all the jobs processed by this
	 * worker will be re-submitted	 	 	 	 
	 * 
	 * @param channel the connection lost client/worker channel	 	 
	 */	 	
	def channelDisconnected( channel: MessageChannel ) {
		if( !channel.isConnected ) {			
			submittedJobs.get( channel ) match {
				case Some( jobs ) =>
					jobs.filterNot{ case (jobHandle, job ) => 
						job.background 
					}.foreach { case (jobHandle, _ ) =>
						submittedJobs( channel ) -= jobHandle 
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
	
	/**
	 * remove a job from the manager
	 * 
	 * @param job the job to be removed	 	 
	 */	 	
	def removeJob( job: Job ) {
		if( submittedJobs.contains( job.from ) ) {
			submittedJobs( job.from ) -= job.jobHandle
		}
		
		if( processingJobs.contains( job.processing ) ) {
			processingJobs( job.processing ) -= job.jobHandle
		}
	}

	/**
	 *  get the pending job count on function
	 *  
	 * @param funcName the function name of job
	 * 
	 * @return the number of pending jobs whose name are funcName	 	 	 	 
	 */	 	
	def getPendingJobCount( funcName: String ) = if( pendingJobs.contains( funcName ) )  pendingJobs( funcName ).size else 0
	
	/**
	 * take a job from pending queue by the function name and set its processing
	 * channel to <code>channel</code> and remove it from the pending queue	 
	 * 
	 * @param funcName the job function name
	 * @param channel the processing worker channel
	 * 
	 * @return the job processed by worker channel or None 	 	 	  	 	 
	 */	 	
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
	
	/**
	 * take a job from pending queue. The function name of job must be one
	 * of name in funcs parameter. The processing channel will be set to
	 * <code>channel</code>. The pending job will be removed from the pending queue 	 	 
	 * 
	 * @param funcs the function names
	 * @param channel the worker channel
	 * 
	 * @return job whose function name is in funcs  	 	 	   	 	 
	 */	 	
	def takeJob( funcs: scala.collection.Set[ String ], channel: MessageChannel ):Option[ Job ] = {
		var job: Option[ Job ] = None
		
		funcs.foreach{ case (funcName) =>
			if( job.isEmpty ) job = takeJob( funcName, channel )
		}
			
		job
	}
	
	/**
	 * get all the submitted & not finished jobs
	 * 
	 * @param all the submitted and not finished jobs 	 	 
	 */
	def getAllJobs: List[ Job ] = {
		var allJobs = List[Job]()
		
		submittedJobs.foreach{ case( channel, jobs ) =>
			jobs.foreach{ case( _, job ) => 
				allJobs = allJobs :+ job 
			} 
		}		
		allJobs 
	}
	
	/**
	 *  get the number of all submitted & not finished jobs
	 *  
	 * @return the number of all submitted & not finished jobs	 	 
	 */	 	
	def getAllJobCount = {
		var count = 0
		submittedJobs.foreach{ case ( _, jobs ) => 
			count += jobs.size 
		}
		count
	}
	
	/**
	 *  get all the client channels
	 *  
	 * @return all the client channels 	 	 
	 */	 	
	def getAllClientChannels = {
		var channels = List[ MessageChannel ]()
		
		submittedJobs.foreach{ case ( channel, _ ) => 
			channels = channel :: channels 
		}
		channels
		
	}
	
	/**
	 * get the queue size limitation for function name 
	 * 
	 * @param funcName the function name 	 	 
	 */
	def getQueueSize( funcName: String ) : Int = if( jobQueueSize.contains( funcName ) ) jobQueueSize(funcName) else -1
	
	/**
	 * set the queue limitation for the function name
	 * 
	 * @param funcName the function name
	 * @param size the queue size	  	 	 
	 */
	def setQueueSize( funcName: String, size: Int ) {
		jobQueueSize += funcName -> size
	}
	
	/**
	 *  check if the queue for funcName is full or not
	 *  
	 * @return true if the queue size reaches the limitation	 	 
	 */	 	
	private def isQueueFull( funcName: String ) : Boolean = pendingJobs.contains( funcName ) && pendingJobs( funcName ).size < getQueueSize( funcName )
		
}

/**
 * receive all the jobs from the clients and dispatch them to workers. Forward
 * the job data got from the workers to the related client.
 *
 * @param executor thread pool used to handle job timeout  
 *    
 * @author Steven Ou   
 */ 
class JobServer( sockAddr: SocketAddress ) extends MessageHandler {
	private val workers = new WorkerManager
	private val jobs = new JobManager
	private val timer = new Timer
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
		
		msg match {
			case EchoReq(data) => handleEchoReq( from, data )
			case CanDo( funcName ) => handleCanDo( from, funcName )
			case CantDo( funcName ) =>  handleCantDo( from, funcName )
			case CanDoTimeout( funcName, timeout ) => handleCanDoTimeout( from, funcName, timeout )
			case ResetAbilities() => handleResetAbilities( from )
			case SubmitJob( funcName, uniqId, data ) => handleSubmitJob( from, funcName, uniqId, data, JobPriority.Normal, false )
			case SubmitJobBg( funcName, uniqId, data ) => handleSubmitJob( from, funcName, uniqId, data, JobPriority.Normal, true )
			case SubmitJobHigh( funcName, uniqId, data ) => handleSubmitJob( from, funcName, uniqId, data, JobPriority.High, false )
			case SubmitJobHighBg( funcName, uniqId, data ) => handleSubmitJob( from, funcName, uniqId, data, JobPriority.High, true )
			case SubmitJobLow( funcName, uniqId, data ) => handleSubmitJob( from, funcName, uniqId, data, JobPriority.Low, false )
			case SubmitJobLowBg( funcName, uniqId, data ) => handleSubmitJob( from, funcName, uniqId, data, JobPriority.Low, true )
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
				if( getPendingJobCountForWorker( from ) > 0 ) from.send( Noop() )
				
			case SetClientId( id ) => workers.setId( from, id )
			case AdminRequest( command, args ) => handleAdminRequest( from, command, args )
			case _ =>     
		}
	}
	
	/**
	 *  If the the client/worker message channel is connection lost, this method will be called
	 *
	 * @param from the client/worker message channel	   	 
	 */	 	
	override def handleDisconnect( from: MessageChannel ) {
		jobs.channelDisconnected( from )
		workers.resetWorkerFunc( from )
	}
	
	/**
	 * shutdown the job server
	 * 
	 * @param graceful true do the graceful shutdown	 	 
	 */	 	
	private def shutdown( graceful: Boolean, callback: => Unit ) {
		stopped = true
				
		executor.submit( new Runnable {
			def run {
				//close the listening socket
				try { serverSockChannel.close } catch { case e:Throwable => }
				
				if( graceful ) doGracefulShutdownCheck( callback ) else {
					closeAllChannels
					callback
				}
			}
		})		
	}
	
	private def doGracefulShutdownCheck( callback: => Unit ) {
		val periodicalChecker = new Runnable {
			def run {
				if( jobs.getAllJobCount == 0 ) {
					closeAllChannels
					callback
				} else {
					executor.submit( this )
				}
			}
		}
		
		executor.submit( periodicalChecker )
		
	}
	
	private def closeAllChannels {
		jobs.getAllClientChannels.foreach{ channel => 
			try { channel.close } catch { case e:Throwable => } 
		}
		workers.getAllWorkerChannels.foreach{ channel => 
			try { channel.close } catch { case e:Throwable => } 
		}
	}
	
	private def getPendingJobCountForWorker( from: MessageChannel ) = {
		var pending = 0
		
		workers.getWorkerFuncs( from ) match {
			case Some( funcs ) => funcs.foreach{ funcName => 
							pending += jobs.getPendingJobCount( funcName ) 
						}
			case _ =>
		}
		pending 
	}
	
	private def handleEchoReq( from: MessageChannel, data: String ) {
		from.send( EchoRes( data ) )
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
	
	private def handleSubmitJob( from: MessageChannel, funcName: String, uniqId: String, data:String, priority: JobPriority.JobPriority, background: Boolean ) {
		if( !stopped ) {
			val job = Job( funcName, UUID.randomUUID.toString, uniqId, data, priority, background, from )		
			handleSubmitJob( from, job )
			from.send( JobCreated( job.jobHandle ) )			
		}
		
	}
	private def handleSubmitJob( from: MessageChannel, job: Job )  {
		jobs.submitJob( job )
		workers.getFuncWorkers( job.funcName ) match {
			case Some( channels ) => 
				channels.foreach{ case ( channel, timeout ) =>
					if( workers.isPreSleep( channel ) ) {
						channel.send( Noop() )
					}  
				}
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
							from.send( JobAssignUniq( job.jobHandle, job.funcName, job.uniqId, job.data ) )
						else from.send( JobAssign( job.jobHandle, job.funcName, job.data ) )
						val timeout = workers.getTimeout( job.funcName, from )
						if( timeout > 0 ) timer.schedule( new TimerTask { def run { handleTimeout( from, job.jobHandle ) } }, timeout * 1000 )
					case _ =>  from.send( NoJob() )
				}
				
			case _ => from.send( NoJob() )
		}
	}
	
	private def handleGetStatus( from: MessageChannel, jobHandle: String ) {
		jobs.getJob( from, jobHandle ) match {
			case Some( job ) => from.send( StatusRes( jobHandle, false, if( job.processing != null ) true else false, job.numerator, job.denominator ) )
			case _ => from.send( StatusRes( jobHandle, true, false, 0, 0 ) )
		}
	}
	
	private def handleWorkDataReq( from: MessageChannel, jobHandle: String, data: String ) {
		jobs.getProcessingJob( from, jobHandle ) match {
			case Some( job ) => if( !job.background ) job.from.send( WorkDataRes( jobHandle, data ) )
			case _ =>
		}
	}
	
	private def handleWorkWarningReq( from: MessageChannel, jobHandle: String, data: String ) {
		jobs.getProcessingJob( from, jobHandle ) match {
			case Some( job ) => if( !job.background ) job.from.send( WorkWarningRes( jobHandle, data ) )
			case _ =>
		}
	}
	
	private def handleWorkStatusReq( from: MessageChannel, jobHandle: String, numerator: Int, denominator: Int ) {
		jobs.getProcessingJob( from, jobHandle ) match {
			case Some( job ) =>
				job.numerator = numerator
				job.denominator = denominator 
				if( !job.background ) job.from.send( WorkStatusRes( jobHandle, numerator, denominator ) )
			case _ =>
		}
	}
	
	private def handleWorkFailReq( from: MessageChannel, jobHandle: String ) {
		jobs.getProcessingJob( from, jobHandle ) match {
			case Some( job ) => 
				if( !job.background ) job.from.send( WorkFailRes( jobHandle ) )
				jobs.removeJob( job )
			case _ =>
		}
	}
	
	private def handleWorkExceptionReq( from: MessageChannel, jobHandle: String, data: String ) {
		jobs.getProcessingJob( from, jobHandle ) match {
			case Some( job ) => 
				if( !job.background ) job.from.send( WorkExceptionRes( jobHandle, data ) )
				jobs.removeJob( job )
			case _ =>
		}
	}
	
	private def handleWorkCompleteReq( from: MessageChannel, jobHandle: String, data: String ) {
		jobs.getProcessingJob( from, jobHandle ) match {
			case Some( job ) => 
				if( !job.background ) job.from.send( WorkCompleteRes( jobHandle, data ) )
				jobs.removeJob( job )
			case _ =>
		}
	}
	
	private def handleTimeout( processingChannel: MessageChannel, jobHandle: String ) {
		executor.submit( new Runnable { def run {
			jobs.getProcessingJob( processingChannel, jobHandle ) match {
				case Some( job ) => 
					if( !job.background ) job.from.send( WorkFailRes( jobHandle ) )
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
			case "shutdown" =>
				val graceful = args.size > 0 && args(0) == "graceful"
				shutdown( graceful, System.exit( 0 ) ) 
			case "version" => respLines = respLines :+ "1.0"
			case _ =>                                       
		}
		
		if( respLines.size > 0 ) from.send( AdminResponse( respLines ) )
	} 
}

object JobServer {
	def apply( sockAddr: SocketAddress) = new JobServer( sockAddr )
	
	def apply( listeningAddr: String, port: Int ) = new JobServer( new InetSocketAddress( listeningAddr, port ) )
	
	def apply( port: Int ) = new JobServer( new InetSocketAddress( port ) )		
}

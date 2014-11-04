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

import org.gearman.channel._
import org.gearman.message._
import java.util.concurrent.{ExecutorService}
import org.specs2.Specification
import org.specs2.mock.Mockito
import org.mockito.Mockito._
import org.mockito.Matchers._
import scala.concurrent._
import scala.concurrent.duration._
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import scala.util._
import scala.util.control.Breaks._

class GearmanClientTest extends Specification with Mockito {

	def is = s2"""
		"channel creation" $testChannelCreation
		"echo" $testEcho
		"get job status" $testGetStatus
		"submit low priority job test" $testSubmitJobLow
		"submit low priority job with timeout test" $testSubmitJobLowWithTimeout
		"submit normal priority job test" $testSubmitJob
		"submit normal priority job with timeout test" $testSubmitJobWithTimeout
		"submit high priority job test" $testSubmitJobHigh	
		"submit high priority job with timeout test" $testSubmitJobHighWithTimeout
		"submit multiple jobs test" $testSubmitMultiJobs
	"""	

	def testChannelCreation = {
		val channel = mock[MessageChannel]
		val client = createClient( channel )
		client.shutdown 
		
		verify( channel ).setMessageHandler( notNull(classOf[MessageHandler]) )
		verify( channel ).open
		verify( channel ).close
		success	
	}
	
	def testEcho = {
		val (channel1, channel2) = MemoryMessageChannel.createPair
		val msgHandler = mock[MessageHandler]
		val client = createClient( channel1 )
		
		openChannel( channel2, msgHandler )
		
		setMessageProcessor( msgHandler ) {		
			case ( msg, channel ) =>
				msg match {
					case EchoReq( data ) => channel.send( EchoRes( data ) )
				}
		}
		
		val f = client.echo( "hello,world!") 
		
		val isSuccess = Await.result( f, Duration.Inf ) == "hello,world!"
		
		verify( msgHandler ).handleMessage( anyObject(), notNull( classOf[MessageChannel ] ) )

        client.shutdown
		channel2.close
		
		if( isSuccess ) success else failure
	}
	
	def testGetStatus = {
		val (channel1, channel2) = MemoryMessageChannel.createPair
		val msgHandler = mock[MessageHandler]
		val client = createClient( channel1 )
		
		openChannel( channel2, msgHandler )
		
		setMessageProcessor( msgHandler ) {		
			case ( msg, channel ) =>
				msg match {
					case GetStatus( jobHandle ) => channel.send( StatusRes(jobHandle, true, true, 10, 100 ) ) 
				}
		}
		
		val f = client.getStatus( "job-1" )
		
		val jobStatus = Await.result( f, Duration.Inf )
		
		verify( msgHandler ).handleMessage( anyObject(), notNull( classOf[MessageChannel ] ) )
		
		channel2.close
		client.shutdown
		if( jobStatus == JobStatus( true, true, 10, 100 ) ) success else failure
		
	}
	
	def testSubmitJob = _testSubmitJob( -1, -1 )
	def testSubmitJobWithTimeout = _testSubmitJob( 1000, 1100 )
	
	private def _testSubmitJob( timeoutMillis: Long = -1, sleepTime: Long = -1 ) = {
		val jobData = "This is a simple Echo"
		val jobId = "job-1"
		
		genericTestSubmitJob( timeoutMillis, 
					sleepTime,
					jobData, 
					jobId,
					{case (msg, channel ) =>
						msg match {
							case SubmitJob( funcName, uid, data ) =>
								channel.send( JobCreated( jobId ) )
								if( sleepTime > 0 ) Thread.sleep( sleepTime )
								channel.send( WorkCompleteRes( jobId, data ) )
						}
					},
					{case (client, p) =>
						client.submitJob( "test", jobData, timeout = timeoutMillis ) {
							case JobComplete( data ) =>  p.trySuccess( data )
							case JobTimeout() => p.trySuccess( "" )
							case _ => p.tryFailure( new Exception( "unexpected") )
						}
					
					})
		
	}
	
	private def genericTestSubmitJob( timeoutMillis: Long, 
						sleepTime: Long,
						data: String,
						jobId: String, 
						msgProcessor: (Message, MessageChannel)=>Unit, 
						submitJob: (GearmanClient, Promise[String] )=>Future[String] ) = {
		val (channel1, channel2) = MemoryMessageChannel.createPair
		val msgHandler = mock[MessageHandler]
		val client = createClient( channel1 )
		
		openChannel( channel2, msgHandler )
		
		setMessageProcessor( msgHandler )( msgProcessor )
		
		val p = Promise[String]
				
		val f = submitJob( client, p )
		
		val hasJobHandle = Await.result( f, Duration.Inf ) == jobId
		val ret = Await.result( p.future, Duration.Inf )
		var isSuccess = if( timeoutMillis > 0 && sleepTime > 0 && sleepTime > timeoutMillis ) ret == "" else ret == data 
                                    	
		verify( msgHandler ).handleMessage( anyObject(), notNull( classOf[MessageChannel ] ) )

        client.shutdown
		channel2.close
		
		if( isSuccess && hasJobHandle ) success else failure
	}
	
	def testSubmitJobLow = _testSubmitJobLow( -1, -1 )
	def testSubmitJobLowWithTimeout = _testSubmitJobLow( 1000, 1100 )
		
	private def _testSubmitJobLow( timeoutMillis: Long, 
						sleepTime: Long ) = {
		val jobData = "This is a simple Echo"
		val jobId = "job-1"
		genericTestSubmitJob( timeoutMillis,
				sleepTime,
				jobData,
				jobId,				 
				{case (msg, channel ) =>
					msg match {
						case SubmitJobLow( funcName, uid, data ) =>
							channel.send( JobCreated( jobId ) )
							if( sleepTime > 0 ) Thread.sleep( sleepTime )
							channel.send( WorkCompleteRes( jobId, data ) )
					}
				},
				{case (client, p) =>
					client.submitJobLow( "test", jobData, timeout = timeoutMillis ) {
						case JobComplete( data ) =>  p.trySuccess( data )
						case JobTimeout() => p.trySuccess( "" )
						case _ => p.tryFailure( new Exception( "unexpected") )
					}
				
				})		
	}
	
	def testSubmitJobHigh = _testSubmitJobHigh( -1, -1 )
	def testSubmitJobHighWithTimeout = _testSubmitJobHigh( 1000, 1100 )
	 
	private def _testSubmitJobHigh( timeoutMillis: Long, 
						sleepTime: Long ) = {
		val jobData = "This is a simple Echo"
		val jobId = "job-1"
		genericTestSubmitJob( timeoutMillis,
				sleepTime,
				jobData,
				jobId,				 
				{case (msg, channel ) =>
					msg match {
						case SubmitJobHigh( funcName, uid, data ) =>
							channel.send( JobCreated( jobId ) )
							if( sleepTime > 0 ) Thread.sleep( sleepTime )
							channel.send( WorkCompleteRes( jobId, data ) )
					}
				},
				{case (client, p) =>
					client.submitJobHigh( "test", jobData, timeout = timeoutMillis ) {
						case JobComplete( data ) =>  p.trySuccess( data )
						case JobTimeout() => p.trySuccess( "" )
						case _ => p.tryFailure( new Exception( "unexpected") )
					}
				
				})		
	}		
	
	def testSubmitMultiJobs = {
		val (channel1, channel2) = MemoryMessageChannel.createPair
		val msgHandler = mock[MessageHandler]
		val client = createClient( channel1 )
		
		openChannel( channel2, msgHandler )
		var jobId = 1
		
		setMessageProcessor( msgHandler ) {					
			case ( msg, channel ) =>
				msg match {
					case SubmitJob( funcName, uid, data ) =>
						channel.send( JobCreated( s"job-$jobId" ) )
						channel.send( WorkCompleteRes( s"job-$jobId", data ) )
						jobId = jobId + 1
				}
		}
		
		var jobs = List.empty[ (String, String, Promise[String], Future[String]) ]
		
        for( i <- 1 until 10 ) {
        	val jobData = s"This is a simple Echo $i"
        	val p = Promise[String]		
			val f = client.submitJob( "test", jobData ) {
				case JobComplete( data ) =>  p.trySuccess( data )
				case _ => p.tryFailure( new Exception( "unexpected") )				
			}
			jobs = jobs :+ ( jobData, s"job-$i", p, f )
		}
		
		var isSuccess = true
		
		jobs.foreach { case ( jobData, jobId, p, f ) =>
			if( Await.result( p.future, Duration.Inf ) != jobData || Await.result( f, Duration.Inf ) != jobId ) {
				isSuccess = false
			} 
		}
						
		verify( msgHandler, times(9) ).handleMessage( anyObject(), notNull( classOf[MessageChannel ] ) )

		client.shutdown
		channel2.close
		if( isSuccess ) success else failure
	}	
	
	private def openChannel( channel: MessageChannel, msgHandler:MessageHandler ) {
		channel.setMessageHandler( msgHandler )
		channel.open
	}
	
	private def setMessageProcessor( msgHandler: MessageHandler ) ( msgProcessor: (Message, MessageChannel)=>Unit ) {
		when( msgHandler.handleMessage( anyObject(), notNull( classOf[MessageChannel ] ) ) ).thenAnswer ( 
			new Answer[Object] {
				def answer(invocation: InvocationOnMock ) : Object = {
					var args = invocation.getArguments
					msgProcessor( args( 0 ).asInstanceOf[ Message ], args( 1 ).asInstanceOf[MessageChannel] )
					null
				}
			}		
		)
	}
	
	private def createClient( channel: MessageChannel ) = {
		val p = Promise[Boolean]
		
		val channelFactory = new MessageChannelFactory {
			def create( executor: ExecutorService, callback: (MessageChannel=>Unit) ) {
				callback( channel )
				p.success( true )
			}
		}
		
		val client = GearmanClient( channelFactory )
		
		Await.ready( p.future, Duration.Inf )
		client
	}	
}
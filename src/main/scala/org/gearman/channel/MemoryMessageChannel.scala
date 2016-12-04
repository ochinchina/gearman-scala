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
package org.gearman.channel

import org.gearman.message._
import java.util.concurrent.LinkedBlockingQueue
import scala.util.control.Breaks._

/**
 * message channel implementation in memory
 * 
 * This channel is designed for unit test purpose
 * 
 * @author Steven Ou    
 */ 
class MemoryMessageChannel private( inQueue: LinkedBlockingQueue[Message], outQueue: LinkedBlockingQueue[Message] ) extends MessageChannel {
	private var closed = true
	private var msgHandler: MessageHandler = null
	private var msgProcThread: Thread = null
	
	def open {
		if( closed ) {
			closed = false
			
			msgProcThread = new Thread {
				override def run {
					takeProcessMessage
				}
				
				private def takeProcessMessage {
					breakable {
						while( true ) {
							try {
								processMessage( inQueue.take )
							}catch {
								case ex:Throwable => 
							}
							if( closed ) {
								processDisconnect
								break
							}
						}
					}
				}
			}
			msgProcThread.start
		}
	}
	
	def close {
		if( closed ) throw new Exception( "channel is closed")
		closed = true
		msgProcThread.interrupt
	}
	
	def send( msg:Message, callback: Option[ Boolean => Unit ] = None ) {
		if( closed ) throw new Exception( "channel is closed")
		outQueue.put( msg )
		if( callback.nonEmpty ) callback.get( true )
	}
	
	def setMessageHandler( msgHandler: MessageHandler ) {
		this.msgHandler = msgHandler		
	}
	
	def isConnected: Boolean = !closed
	
	def getAddress: String = "memory"
	
	private def processMessage( msg: Message ) {
		msgHandler.handleMessage( msg, this )
	}
	
	private def processDisconnect {
		msgHandler.handleDisconnect( this )
	}

}

/** Factory for [[MemoryMessageChannel]]*/
object MemoryMessageChannel {

	/**
	 *  create two channels connected each other through memory
	 *  
	 * @return a tuple includes the two channels	 	 
	 */	 	
	def createPair: ( MemoryMessageChannel, MemoryMessageChannel ) = {
		val queue_1 = new LinkedBlockingQueue[Message]
		val queue_2 = new LinkedBlockingQueue[Message]
		
		( new MemoryMessageChannel( queue_1, queue_2 ), new MemoryMessageChannel( queue_2, queue_1 ) ) 
	}
}

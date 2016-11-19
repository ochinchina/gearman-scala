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

import org.gearman.message.Message
import java.util.concurrent.ExecutorService

/**
 * message handler to handle the gearman message got from the peer through 
 * message channel
 *
 * @author Steven Ou 
 */  
trait MessageHandler {
	/**
	 * handle the received message from peer
	 * 
	 * @param msg the received gearman message
	 * @param from the message channel (represents peer)	 	 	  
	 */	 	
	def handleMessage( msg: Message, from: MessageChannel )
	
	/**
	 * Connection is lost from the message channel
	 * 
	 * @param from the connection lost message channel	 	 
	 */	 	
	def handleDisconnect( from: MessageChannel )
}

/**
 * presents a bi-direction communication channel between two peers.
 * 
 * When the client/worker starts, it will try to create a connection to the
 * gearman server. If the communication channel is established successfully,
 * a message channel will be created between the client/worker and gearman server.
 * 
 * The client/worker and the gearman server will get the message channel to present
 * the established communication channel        
 * 
 * @author Steven Ou  
 */ 
trait MessageChannel {
	/**
	 * open the message channel. Before calling this method, the setMessageHandler 
	 * must be called to set the message handler.
	 */	 	
	def open
	
	/**
	 * close the message channel
	 */	 	
	def close
	
	/**
	 * send a gearman message to peer with optional callback method. If callback
	 * is provided, the callback will get the message send status ( true for 
	 * successful sending and false for sending failure)
	 * 
	 * @param msg the message sent to peer
	 * @param callback optional callback to get the send status	 	 	 	 	 
	 */	 	
	def send( msg:Message, callback: Option[ Boolean => Unit ] = None )
	
	/**
	 * set the message handler for this message channel. All received gearman
	 * message from this channel will be dispatched to the message handler.
	 * 
	 * @param msgHandler the message handler to handle the received message 	 	 	 
	 */	 	       
	def setMessageHandler( msgHandler: MessageHandler )
	
	/**
	 * return the status of the message channel
	 * 
	 * @return true if the connection is still connected, false if the connection
	 * is broken	 	 	 
	 */	 	
	def isConnected: Boolean
	
	/**
	 * get the IP address of the peer
	 * 
	 * @return the peer IP address	 	 
	 */	 	
	def getAddress: String
}

/**
 * create a MessageChannel in asyn mode. After a channel is created, the {@code callback}
 * should be called in the {@code executor} 
 *
 * @param executor the executor
 * @param callback after the message channel is created, notify the user the channel
 * is ready by calling {@code callback}    
 */  
trait MessageChannelFactory {
	/**
	 *  create a channel and notify user if the channel is created through {@code callback}.
	 *  The {@code callback} should be executed in the {@code executor}
	 *  
	 * @param executor the executor that the {@code callback} will be called
	 * @param callback notify user after the channel is created	 	 	 	 	 
	 *
	 */	 	 	
	def create( executor: ExecutorService, callback: MessageChannel=> Unit )
}
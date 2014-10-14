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
import java.net.{SocketAddress, InetSocketAddress}
import java.util.concurrent.{Executors}
import org.gearman.channel._
						
class GearmanServer( sockAddr: SocketAddress ) {
	val executor = Executors.newFixedThreadPool( 1 ) 
	val jobServer = new JobServer( executor )
	 
	def start {
		AsyncSockMessageChannel.accept( sockAddr, (channel:MessageChannel) => {
			channel.setMessageHandler( jobServer )
			channel.open
		}, Some( executor ) ) 		
	}
	
	
	
}

object GearmanServer {
	def apply( sockAddr: SocketAddress) = {
		new GearmanServer( sockAddr ) 
	}
	
	def apply( listeningAddr: String, port: Int ): GearmanServer = {
		apply( new InetSocketAddress( listeningAddr, port ) );
	}
	
	def apply( port: Int ): GearmanServer = {
		apply( new InetSocketAddress( port ) );
	}
	
	def main( args: Array[String] ) {
		val server = GearmanServer( args(0), args(1).toInt )
		server.start
		while( true ) {
			Thread.sleep( 1000 )
		}
	}
}



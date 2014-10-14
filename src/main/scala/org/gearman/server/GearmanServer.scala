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
			channel.start
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



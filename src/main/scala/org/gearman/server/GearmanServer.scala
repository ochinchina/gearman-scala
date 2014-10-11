package org.gearman.server

import org.gearman.message._

import java.nio.channels.{AsynchronousServerSocketChannel,
						AsynchronousSocketChannel,
						AsynchronousChannelGroup,
						CompletionHandler}
import java.nio.ByteBuffer
import java.net.{ SocketAddress, InetSocketAddress }
import java.io.{ByteArrayInputStream, 
			DataInputStream,
			ByteArrayOutputStream,
			DataOutputStream}
import scala.util.control.Breaks._
import org.gearman.channel._
						
class GearmanServer( sockAddr: SocketAddress ) {
	val serverSock = AsynchronousServerSocketChannel.open()
	val jobServer = new JobServer 
	 
	def start {
		AsyncSockMessageChannel.accept( sockAddr, (channel:MessageChannel)=> {
			channel.setMessageHandler( jobServer )
			channel.start
		}) 		
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
		//val server = GearmanServer( args(0), args(1).toInt )
		val server = GearmanServer( "127.0.0.1", 3333 )
		server.start
		while( true ) {
			Thread.sleep( 1000 )
		}
	}
}



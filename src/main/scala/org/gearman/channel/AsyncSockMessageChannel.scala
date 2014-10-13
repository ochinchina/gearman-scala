package org.gearman.channel

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
import java.util.concurrent.{ExecutorService}
import scala.util.control.Breaks._
import org.gearman.message._

class MessageBuffer {
	import Array._

	var msgBuf = ofDim[Byte](1024*1024)
	var msgBufLen = 0
	
	def add( data: Array[Byte], len: Int ) {
		if( (len + msgBufLen ) > msgBuf.length ) {
			val tmp = ofDim[Byte]( msgBuf.length + len )
			copy( msgBuf, 0, tmp, 0, msgBufLen )
			copy( data, 0, tmp, msgBufLen, len )
			msgBufLen += len
			msgBuf = tmp					
		} else {
			copy( data, 0, msgBuf, msgBufLen, len)
			msgBufLen += len
		}

	}
	
	def extractMsg: Message = if( msgBuf(0) == 0 ) extractBinMsg else extractAdminMsg
		
	private def extractBinMsg(): Message = {
		var msg: Message = null
		 
		if( msgBufLen >= 12 ) {
			var dis = new DataInputStream( new ByteArrayInputStream( msgBuf, 8, 4 ) )
			val len = dis.readInt
			println( "len=" + len )
			if( (len + 12) <= msgBufLen ) {
				dis = new DataInputStream( new ByteArrayInputStream( msgBuf, 0, len + 12 ) )
				msgBufLen -= (len + 12)
				
				try {
					msg = Message.readFrom( dis )
				}catch {
					case ex: Exception => ex.printStackTrace
				}
				copy( msgBuf, len + 12, msgBuf, 0, msgBufLen )
			}
		}
		msg
	}
	
	
	private def extractAdminMsg(): Message = {
		var msg: String = null
		
		var i = 0
		breakable {
			while( i < msgBufLen ) {
				if( msgBuf(i) == '\n' ) break else i += 1
			}
		}
		
		if( i < msgBufLen ) {
			i += 1
			msg = new String( msgBuf, 0, i, "UTF-8" )
			copy( msgBuf, i, msgBuf, 0, msgBufLen - i )
			msgBufLen -= i
			AdminRequest.parse( msg.trim )			
		} else null
	}
	
	
	
}

class AsyncSockMessageChannel( sockChannel: AsynchronousSocketChannel ) extends MessageChannel {
	
	var msgBuf = new MessageBuffer
	var msgHandler: MessageHandler = null
	var connected = true;
	val channel = new AsynchronousSocketChannelWrapper( sockChannel )
	
	def start {
		startRead
	}
	
	def send( msg:Message ) {
		val bos = new ByteArrayOutputStream
		val dos = new DataOutputStream( bos )
		msg.writeTo( dos )
		
		send( ByteBuffer.wrap( bos.toByteArray ) )
	}
	       
	def setMessageHandler( msgHandler: MessageHandler ) {
		this.msgHandler = msgHandler
	}
	
	def isConnected: Boolean = connected
	
	def getAddress: String = {
		val remoteAddr = sockChannel.getRemoteAddress
		
		if( remoteAddr.isInstanceOf[ InetSocketAddress ] ) {
			remoteAddr.asInstanceOf[ InetSocketAddress ].getAddress.getHostAddress
		} else "not bound"
	}
	
	private def send( buf: ByteBuffer ) {
		channel.write( buf, null, new CompletionHandler[Integer, Void]{
			def completed(bytesWritten: Integer, data: Void ) {				
			}
			
			def failed(exc:Throwable, data: Void ) {
			}
		})
	}
	
	private def startRead {
		val buf = ByteBuffer.allocate(2048)
		
		channel.read( buf, null, new CompletionHandler[Integer, Void]{
			def completed( bytesRead: Integer, data: Void ) {
				buf.flip
				if( bytesRead > 0 ) {
					msgBuf.add( buf.array, bytesRead )
					processMsgBuf
				}
				startRead
			}
			
			def failed( exc: Throwable, data: Void ) {
				handleDisconnect
			}
		})
	}
	
	private def processMsgBuf {
		breakable {
			while( true ) {
				val msg = msgBuf.extractMsg
				if( msg == null ) break else msgHandler.handleMessage( msg, this )
			}
		} 
	}
		
	private def handleDisconnect() {
		connected = false
		msgHandler.handleDisconnect( this )
	}
}

object AsyncSockMessageChannel {
	def accept( sockAddr: SocketAddress, callback: ((MessageChannel) )=>Unit, exectutor: ExecutorService ) {
		val serverSock = if( exectutor == null ) AsynchronousServerSocketChannel.open() else AsynchronousServerSocketChannel.open( AsynchronousChannelGroup.withThreadPool( exectutor ) )
		serverSock.bind( sockAddr )
		serverSock.accept( null, new CompletionHandler[AsynchronousSocketChannel, Void]{
			def completed( sockChannel: AsynchronousSocketChannel, data: Void ) {
				callback( new AsyncSockMessageChannel( sockChannel ) )				
				serverSock.accept( null, this )
			}
			
			def failed( ex: Throwable, data: Void ) {
			}
		})
	}
	
	def connect( sockAddr: SocketAddress ): MessageChannel  = {
		val sockChannel = AsynchronousSocketChannel.open
		val connectedChannel = new ValueNotifier[MessageChannel]
		
		sockChannel.connect( sockAddr, null, new CompletionHandler[Void, Void] {
			def completed( result:Void , attachment:Void  ) {
				connectedChannel.notifyValue( new AsyncSockMessageChannel( sockChannel ) )
				
			}
			
			def failed( ex: Throwable , attachment:Void) {
				connectedChannel.notifyValue( null )
			}
		})
		connectedChannel.waitValue
	}
}


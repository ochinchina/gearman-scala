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

/**
 * manage the gearman message buffer
 * 
 * @author Steven Ou  
 */ 
class MessageBuffer {
	import Array._

	var msgBuf = ofDim[Byte](1024*1024)
	var msgBufLen = 0
	
	/**
	 *  add data to this buffer
	 */	 	
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
	
	/**
	 *  extract gearman message from buffer
	 */	 	
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

/**
 * MessageChannel implementation with socket
 * 
 * @author Steven Ou   
 */ 

class AsyncSockMessageChannel( sockChannel: AsynchronousSocketChannel ) extends MessageChannel {
	
	var msgBuf = new MessageBuffer
	var msgHandler: MessageHandler = null
	var connected = true;
	val channel = new AsynchronousSocketChannelWrapper( sockChannel )
	
	def open {
		startRead
	}
	
	def close {
		try {
			sockChannel.close
		} catch {
			case e: Throwable =>
		}
	}
	
	def send( msg:Message, callback: Option[ Boolean => Unit ] = None ) {
		val bos = new ByteArrayOutputStream
		val dos = new DataOutputStream( bos )
		msg.writeTo( dos )
		
		println( "send " + msg )
		send( ByteBuffer.wrap( bos.toByteArray ), callback )
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
	
	private def send( buf: ByteBuffer, callback: Option[ Boolean => Unit ] ) {
		channel.write( buf, null, new CompletionHandler[Integer, Void]{
			def completed(bytesWritten: Integer, data: Void ) {
				if( callback.nonEmpty) callback.get( true )				
			}
			
			def failed(exc:Throwable, data: Void ) {
				if( callback.nonEmpty ) callback.get( false )
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
				println( "diconnected")
				handleDisconnect
			}
		})
	}
	
	private def processMsgBuf {
		breakable {
			while( true ) {
				val msg = msgBuf.extractMsg
				if( msg == null ) 
					break 
				else {
					println( "receive " + msg )
					try {
						msgHandler.handleMessage( msg, this )
					}catch{
						case e:Throwable => e.printStackTrace
					}
				}
			}
		} 
	}
		
	private def handleDisconnect() {
		connected = false
		try {
			msgHandler.handleDisconnect( this )
		}catch {
			case e:Throwable => e.printStackTrace
		}
	}
}

object AsyncSockMessageChannel {
	def accept( sockAddr: SocketAddress, callback: ((MessageChannel) )=>Unit, exectutor: Option[ExecutorService] = None ) = {
		val serverSock = if( exectutor.isEmpty ) AsynchronousServerSocketChannel.open() else AsynchronousServerSocketChannel.open( AsynchronousChannelGroup.withThreadPool( exectutor.get ) )
		serverSock.bind( sockAddr )
		serverSock.accept( null, new CompletionHandler[AsynchronousSocketChannel, Void]{
			def completed( sockChannel: AsynchronousSocketChannel, data: Void ) {
				callback( new AsyncSockMessageChannel( sockChannel ) )				
				serverSock.accept( null, this )
			}
			
			def failed( ex: Throwable, data: Void ) {
			}
		})
		serverSock
	}
	
	def asyncConnect( sockAddr: SocketAddress, callback: ( MessageChannel)=>Unit, exectutor: Option[ExecutorService] = None ) {
		val sockChannel = if( exectutor.isEmpty ) AsynchronousSocketChannel.open else AsynchronousSocketChannel.open( AsynchronousChannelGroup.withThreadPool( exectutor.get ) )
		
		sockChannel.connect( sockAddr, null, new CompletionHandler[Void, Void] {
			def completed( result:Void , attachment:Void  ) {				
				callback( new AsyncSockMessageChannel( sockChannel ) )
				
			}
			
			def failed( ex: Throwable , attachment:Void) {
				callback( null )
			}
		})
	}
	
	def connect( sockAddr: SocketAddress, exectutor: Option[ExecutorService] = None ): MessageChannel  = {
		val sockChannel = if( exectutor.isEmpty ) AsynchronousSocketChannel.open else AsynchronousSocketChannel.open( AsynchronousChannelGroup.withThreadPool( exectutor.get ) )
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


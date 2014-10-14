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

import java.nio.{ByteBuffer}
import java.util.{LinkedList}
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.{TimeUnit}
 
class AsynchronousSocketChannelWrapper( channel: AsynchronousSocketChannel ) {

	trait Operation {
		def execute()
	}
	
	class ProxyCompletionHandler[V,A]( handler: CompletionHandler[V, A], ops: LinkedList[ Operation ] ) extends CompletionHandler[V, A] {
		def completed( result: V, attachment: A ) {
			try { 
				handler.completed( result, attachment )
			}catch {
				case _:Throwable =>
			}
			execNextOperation( ops )
		}
		
		def failed(ex: Throwable, attachment:A ) {
			try {
				handler.failed( ex, attachment )
			}catch {
				case _: Throwable =>
			}
			execNextOperation( ops )
		}
		
	}
	
	class FirstReadOperation[A]( dsts: Array[ByteBuffer],
			offset:Int, 
			length:Int, 
			timeout:Long, 
			unit:TimeUnit, 
			attachment: A, 
			handler: CompletionHandler[java.lang.Long, A] ) extends Operation {
						
			override def execute() {
				val proxyHandler = new ProxyCompletionHandler[java.lang.Long, A]( handler, readOperations)
				try {
					channel.read[A]( dsts, offset, length, timeout, unit, attachment, proxyHandler )
				}catch {
					case ex:Throwable => 
						proxyHandler.failed( ex, attachment )
				}
			}			
	}
	
	class SecondReadOperation[A]( dst:ByteBuffer,
            attachment:A,
            handler:CompletionHandler[Integer, A ] ) extends Operation {
        
        
		def execute() {
        	val proxyHandler = new ProxyCompletionHandler[java.lang.Integer, A]( handler, readOperations)
        	
			try {
				channel.read[A]( dst, attachment, proxyHandler )
			}catch {
				case ex:Throwable => 
					proxyHandler.failed( ex, attachment )
			}
		}
	}
	
	class ThirdReadOperation[A]( dst:ByteBuffer,
            timeout: Long,
            unit:TimeUnit,
            attachment:A ,
            handler:CompletionHandler[java.lang.Integer, A ] ) extends Operation {
                    
		def execute() {
			val proxyHandler = new ProxyCompletionHandler[java.lang.Integer, A]( handler, readOperations)
			try {
				channel.read[A]( dst, timeout, unit, attachment, proxyHandler )
			}catch {
				case ex:Throwable => 
					proxyHandler.failed( ex, attachment )
			}
		}
	}
	
	class FirstWriteOperation[A]( srcs: Array[ByteBuffer] ,
             offset: Int,
             length: Int,
             timeout: Long,
             unit: TimeUnit,
             attachment: A,
             handler: CompletionHandler[java.lang.Long,A] ) extends Operation {
             
    	def execute() {
    		val proxyHandler = new ProxyCompletionHandler[java.lang.Long, A]( handler, writeOperations );
    		try {
    			channel.write[A]( srcs, offset, length, timeout, unit, attachment, proxyHandler )
			}catch {
				case ex: Throwable =>
					proxyHandler.failed( ex, attachment )
			}
		}
	}
	
	class SecondWriteOperation[A]( src: ByteBuffer,             
             attachment: A,
             handler: CompletionHandler[java.lang.Integer,A] ) extends Operation {
             
    	def execute() {
    		val proxyHandler = new ProxyCompletionHandler[java.lang.Integer, A]( handler, writeOperations );
    		try {
    			channel.write[A]( src, attachment, proxyHandler )
			}catch {
				case ex: Throwable =>
					proxyHandler.failed( ex, attachment )
			}
		}
	}
	
	class ThirdWriteOperation[A]( src: ByteBuffer,
			timeout: Long,
             unit: TimeUnit,             
             attachment: A,
             handler: CompletionHandler[java.lang.Integer,A] ) extends Operation {
             
    	def execute() {
    		val proxyHanlder = new ProxyCompletionHandler[java.lang.Integer, A]( handler, writeOperations );
    		try {
    			channel.write[A]( src, timeout, unit, attachment, proxyHanlder )
			}catch {
				case ex: Throwable =>
					proxyHanlder.failed( ex, attachment )
			}
		}
	}
	
	val readOperations = new LinkedList[ Operation ]
	val writeOperations = new LinkedList[ Operation ]
	
	def read[A]( dsts: Array[ByteBuffer],
			offset:Int, 
			length:Int, 
			timeout:Long, 
			unit:TimeUnit, 
			attachment: A, 
			handler: CompletionHandler[java.lang.Long, A] ) {
			
			addReadOperation( new FirstReadOperation[A]( dsts, offset, length, timeout, unit, attachment, handler ) )
	}
	
	def read[A](dst:ByteBuffer,
            attachment:A,
            handler:CompletionHandler[Integer, A ] ) {
		addReadOperation( new SecondReadOperation[A]( dst, attachment, handler ) )
	}
	
	def read[A]( dst:ByteBuffer,
            timeout: Long,
            unit:TimeUnit,
            attachment:A ,
            handler:CompletionHandler[java.lang.Integer, A ] ) {
		addReadOperation( new ThirdReadOperation[A]( dst, timeout, unit, attachment, handler ) )            
	}
	
	def write[A] ( srcs: Array[ByteBuffer] ,
             offset: Int,
             length: Int,
             timeout: Long,
             unit: TimeUnit,
             attachment: A,
             handler: CompletionHandler[java.lang.Long,A] ) {
		addWriteOpeartion( new FirstWriteOperation( srcs, offset, length, timeout, unit, attachment, handler ) )
	}
	
	def write[A] ( src:ByteBuffer,
             attachment: A ,
             handler: CompletionHandler[java.lang.Integer,A] ) {
		addWriteOpeartion( new SecondWriteOperation( src, attachment, handler ) )
	}
	
	def write[A] ( src:ByteBuffer,
             timeout: Long,
             unit: TimeUnit,
             attachment: A ,
             handler: CompletionHandler[java.lang.Integer,A] ) {
		addWriteOpeartion( new ThirdWriteOperation( src, timeout, unit, attachment, handler ) )
	}
	
	private def addReadOperation( op: Operation ) {
		addOperation( op, readOperations )
	}
	
	private def addWriteOpeartion( op: Operation ) {
		addOperation( op, writeOperations )
	}
	
	private def addOperation( op: Operation, ops: LinkedList[ Operation] ) {
		var execOp: Operation = null
		
		ops.synchronized {
			ops.add( op )
			if( ops.size == 1 ) {
				execOp = ops.getFirst
			}
		}
		
		if( execOp != null ) execOp.execute
	}
	
	private def execNextOperation( ops: LinkedList[ Operation ] ) {
		var nextExecOps: Operation = null
		 
		ops.synchronized {
			ops.removeFirst
			if( !ops.isEmpty ) nextExecOps = ops.getFirst 
		}
		
		if( nextExecOps != null ) nextExecOps.execute
	}
}

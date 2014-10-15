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
package org.gearman.example

import org.gearman.client._

object GearmanClientExample {

	def main( args: Array[String] ) {
		if( args.size != 1 ) {
			printUsage
			return
		}
		val client = new GearmanClient( args( 0 ) )
		client.start
		println( client.echo( "test") )
		
		client.submitJob( "test", "1234", "hello", new JobCallback {
			def data( data: String ) {
				println( "data:" + data )
			}
			def warning( data: String ) {
			}
			def status( numerator: Int, denominator: Int ) {
			}
			def complete( data: String ) {
				println( "complete:" + data )
				println( "complete, Thread---" + Thread.currentThread.getId )
				
				client.submitJob( "test", "12345", "hello", this )
			}
			def fail {
				client.submitJob( "test", "12345", "hello", this )
			}
			def exception( data: String ) {
				client.submitJob( "test", "12345", "hello", this )
			}
			
			def timeout {
				client.submitJob( "test", "12345", "hello", this )
			}
			
			def connectionLost {
				client.submitJob( "test", "12345", "hello", this )
			}
		})
		
		while( true ) {
			Thread.sleep( 1000 )
		}
	}
	
	private def printUsage {
		println( "Usage: java -cp <classpath> org.gearman.example.GearmanClientExample servers")
		println( """servers server address in format "server1:port1,server2:port2,...,servern:portn" """)
	}
}
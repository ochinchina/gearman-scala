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
package org.gearman.message

import org.scalatest._
import java.io.{ByteArrayOutputStream,
			ByteArrayInputStream,
			DataOutputStream,
			DataInputStream}

class MessageTest extends FlatSpec  with Matchers {
		it should "echo request codec" in {
			val msg = codecMessage( new EchoReq( "hello") )
			msg.asInstanceOf[ EchoReq ].data should be ("hello")
		}
		
		it should "echo response codec" in {
			val msg = codecMessage( new EchoRes( "hello") )
			msg.asInstanceOf[ EchoRes ].data should be ("hello")
		}
		
		it should "CanDo request codec" in {
			val msg = codecMessage( new CanDo( "func") )
			msg.asInstanceOf[ CanDo ].funcName should be ("func")
		}
		
		it should "CantDo request codec" in {
			val msg = codecMessage( new CantDo( "func") )
			msg.asInstanceOf[ CantDo ].funcName should be ("func")
		}
		
		it should "AdminRequest codec" in {
			val msg = AdminRequest( "command arg1 arg2 arg3")
			
			val adminReq = msg.asInstanceOf[AdminRequest]
			
			adminReq.command should be( "command" )
			
			adminReq.args.size should be (3)
			
			adminReq.args(0) should be ("arg1")
			adminReq.args(1) should be ("arg2")
			adminReq.args(2) should be ("arg3")
		}
	
	private def codecMessage( msg: Message ): Message = {
			val arrayOut = new ByteArrayOutputStream 
			msg.writeTo( new DataOutputStream( arrayOut ) )
			Message.readFrom( new DataInputStream( new ByteArrayInputStream( arrayOut.toByteArray ) ) )
	} 
}

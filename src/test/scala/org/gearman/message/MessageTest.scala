package org.gearman.message

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import java.io.{ByteArrayOutputStream,
			ByteArrayInputStream,
			DataOutputStream,
			DataInputStream}

class MessageTest extends FunSpec with ShouldMatchers {
	describe("Message") {
		it ( "echo request codec") {
			val msg = codecMessage( new EchoReq( "hello") )
			msg.asInstanceOf[ EchoReq ].data should be ("hello")
		}
		
		it ( "echo response codec") {
			val msg = codecMessage( new EchoRes( "hello") )
			msg.asInstanceOf[ EchoRes ].data should be ("hello")
		}
		
		it ( "CanDo request codec") {
			val msg = codecMessage( new CanDo( "func") )
			msg.asInstanceOf[ CanDo ].funcName should be ("func")
		}
		
		it ( "CantDo request codec") {
			val msg = codecMessage( new CantDo( "func") )
			msg.asInstanceOf[ CantDo ].funcName should be ("func")
		}
		
		it( "AdminRequest codec") {
			val msg = AdminRequest.parse( "command arg1 arg2 arg3")
			
			val adminReq = msg.asInstanceOf[AdminRequest]
			
			adminReq.command should be( "command" )
			
			adminReq.args.size should be (3)
			
			adminReq.args(0) should be ("arg1")
			adminReq.args(1) should be ("arg2")
			adminReq.args(2) should be ("arg3")
		}
	}
	
	private def codecMessage( msg: Message ): Message = {
			val arrayOut = new ByteArrayOutputStream 
			msg.writeTo( new DataOutputStream( arrayOut ) )
			Message.readFrom( new DataInputStream( new ByteArrayInputStream( arrayOut.toByteArray ) ) )
	} 
}
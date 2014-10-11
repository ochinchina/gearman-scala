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
	}
	
	private def codecMessage( msg: Message ): Message = {
			val arrayOut = new ByteArrayOutputStream 
			msg.writeTo( new DataOutputStream( arrayOut ) )
			Message.readFrom( new DataInputStream( new ByteArrayInputStream( arrayOut.toByteArray ) ) )
	} 
}
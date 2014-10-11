package org.gearman.channel

import org.gearman.message._

trait MessageHandler {
	def handleMessage( msg: Message, from: MessageChannel )
	def handleDisconnect( from: MessageChannel )
	

}

trait MessageChannel {
	def start
	def send( msg:Message )       
	def setMessageHandler( msgHandler: MessageHandler )
	def isConnected: Boolean
}
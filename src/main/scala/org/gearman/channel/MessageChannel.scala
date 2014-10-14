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

import org.gearman.message._

trait MessageHandler {
	def handleMessage( msg: Message, from: MessageChannel )
	def handleDisconnect( from: MessageChannel )
	

}

trait MessageChannel {
	def open
	def close
	def send( msg:Message )       
	def setMessageHandler( msgHandler: MessageHandler )
	def isConnected: Boolean
	def getAddress: String
}
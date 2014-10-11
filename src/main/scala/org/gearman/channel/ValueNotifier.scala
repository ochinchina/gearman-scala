package org.gearman.channel

import scala.util.control.Breaks._

class ValueNotifier[T] {
	var notified = false
	var value: T = _
	val mutex = new java.lang.Object
	
	def waitValue: T = {
		breakable {
			while( true ) {
				mutex.synchronized {
					if( !notified ) mutex.wait
					if( notified ) break						
				}
			}
		}
		value
	}
	
	def notifyValue( value: T ) {
		mutex.synchronized {
			this.value = value
			this.notified = true
			mutex.notify
		}
	}
}
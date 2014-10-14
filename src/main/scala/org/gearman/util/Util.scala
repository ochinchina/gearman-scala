package org.gearman.util

import java.net.{InetSocketAddress}

object Util {
	def parseAddressList( addrs: String ): List[ InetSocketAddress ] = {	
		var addrList = List[ InetSocketAddress ]()
		addrs.split( "," ).foreach { addr =>
			val index = addr.lastIndexOf( ':')
			if( index != -1 ) {
				try {
					addrList = addrList :+ new InetSocketAddress( addr.substring( 0, index ), addr.substring( index + 1 ).toInt )
				} catch {
					case e:Throwable => e.printStackTrace
				}
			}
		}
		addrList
	}
}
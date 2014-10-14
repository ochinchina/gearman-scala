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
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
package org.gearman.server

import org.gearman.channel._
import scala.collection.mutable.Map    
import scala.collection.immutable.List
import scala.collection.mutable.Set

case class WorkerFuncInfo( funcName: String, timeout: Int = -1)

class WorkerManager {		
	private val workers = Map[MessageChannel, List[WorkerFuncInfo] ]()
	private val workIds = Map[MessageChannel, String ]()
	private val sleepWorkers = Set[MessageChannel]()
	
	def addFunc( worker: MessageChannel, funcName: String, timeout: Int = -1 ) = {
		//remove the function information at first
		removeFunc( worker, funcName )

		//add the function information		
		workers.get(worker) match {
			case Some( funcList ) => workers += ( worker -> ( funcList :+ WorkerFuncInfo( funcName, timeout ) ) )
			case _ => workers += (worker -> List( WorkerFuncInfo( funcName, timeout ) ) )			

		}
	}
	
	/**
	 *  return the functions supported by the worker
	 *
	 */	 	 	
	def getFuncs(worker: MessageChannel): List[WorkerFuncInfo] = workers.getOrElse(worker, List[WorkerFuncInfo]() )
	
	def foreachWorker( func: (MessageChannel, List[WorkerFuncInfo] )=>Unit ) {
		workers.foreach{ case (k,v) => func(k,v) }
	}
	
	def foreachFunc( func: (String, List[MessageChannel]) => Unit ) {
		val funcClients = Map[String, List[MessageChannel] ]()
		workers.foreach { 
			case (client, funcs ) =>
				funcs.foreach{ 
					case funcInfo => 
						funcClients += ( funcInfo.funcName -> (funcClients.getOrElse(funcInfo.funcName, List[MessageChannel]() ) :+ client) )
				} 							
		}
		funcClients.foreach{
			case (funcName, clients ) => func( funcName, clients )
		}
	}
	
	def remove( worker: MessageChannel ) = {
		workers -= worker
		workIds -= worker
	}
	
	def sleep( worker: MessageChannel )= {
	  sleepWorkers += worker
	}
	
	def wakeup( funcName: String ): List[MessageChannel] = {
	  val channels = Set[MessageChannel]()
	  workers.foreach{ case (channel, funcInfos) => {
	    funcInfos.foreach { funcInfo=>
	      if( funcInfo.funcName == funcName ) channels += channel
	    }
	  }}
	  val ret = sleepWorkers.intersect( channels ).toList
	  ret.foreach( channel=>sleepWorkers.remove(channel) )
	  ret
	}
	
	def removeFunc( worker: MessageChannel, funcName: String ) = {
		workers.get(worker) match {
			case Some( funcList ) =>
				val r = funcList.filterNot( _.funcName == funcName )
				if( r.isEmpty ) {
					workers -= worker
				} else {
					workers += ( worker -> r )
				}
			case _ =>
		}
	}
	
	def setId( worker: MessageChannel, id: String ) = {
		workIds += ( worker -> id )
	}
	
	def getId( worker: MessageChannel ): Option[String] = workIds.get( worker )

}
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

import org.gearman.message.JobPriority
import scala.collection.immutable.List

class JobList {
	private var highPrioJobs = List[Job]()
	private var normalPrioJobs = List[Job]()
	private var lowPrioJobs = List[Job]()
	private val jobQueueSizer = new JobQueueSizer()
	
	def add( job: Job ): Boolean = {
	    if( jobQueueSizer.tryToAddJob( job ) && find( _.uniqId == job.uniqId ).isEmpty ) {
			job.priority match {
				case JobPriority.High => highPrioJobs = highPrioJobs :+ job
				case JobPriority.Low => lowPrioJobs = lowPrioJobs :+ job
				case _ => normalPrioJobs = normalPrioJobs :+ job
			}
			return true
		}
		return false
	}
	
	def find( pred: Job=>Boolean ): Option[Job] = 
		highPrioJobs.find( pred ).orElse( normalPrioJobs.find( pred ).orElse( lowPrioJobs.find( pred ) ) )
	
	def remove( job: Job ): Boolean = {
	    jobQueueSizer.tryToRemoveJob( job )
		job.priority match {
			case JobPriority.High => 
			if( highPrioJobs.contains( job ) ) {
			    highPrioJobs = highPrioJobs.filterNot( _ == job )
			    return true
			} 
			case JobPriority.Low => 
			if( lowPrioJobs.contains( job ) )  {
			    lowPrioJobs = lowPrioJobs.filterNot( _ == job )
			    return true
			}
			case _ => if( normalPrioJobs.contains( job ) ) {
			    normalPrioJobs = normalPrioJobs.filterNot( _ == job )
			    return true
			}
		}
		return false
	}
	
	
	def foreach( callback: Job=> Unit ) {
		highPrioJobs.foreach( callback )
		normalPrioJobs.foreach( callback )
		lowPrioJobs.foreach( callback )
	}
	
	def map[B]( p: Job=>B ): List[B] = highPrioJobs.map( p(_) ) ::: normalPrioJobs.map( p(_) ) ::: lowPrioJobs.map( p(_) )
	
	def setQueueSize( funcName: String, maxSize: Int ) = {
	    jobQueueSizer.setMaxSize( funcName, maxSize )
		
	}
	
	def setQueueSize( funcName: String, jobPriority: JobPriority.JobPriority, maxSize: Int ) = {
	    jobQueueSizer.setMaxSize( funcName, jobPriority, maxSize )
	}
	
	def size = highPrioJobs.size + normalPrioJobs.size + lowPrioJobs.size
	
	
		
}
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

import scala.collection.mutable.Map
import org.gearman.message._


class JobQueueSizer {
  val highPrioFuncMaxSize = Map[String,Int]()
  val normalPrioFuncMaxSize = Map[String,Int]()
  val lowPrioFuncMaxSize = Map[String,Int]()
  
  val highPrioJobSize = Map[String,Int]()
  val normalPrioJobSize = Map[String,Int]()
  val lowPrioJobSize = Map[String,Int]()
  
  def setMaxSize(funcName:String, size: Int ) = {
    highPrioFuncMaxSize += (funcName->size)
    normalPrioFuncMaxSize += (funcName->size)
    lowPrioFuncMaxSize += (funcName->size)
  }
  
  
  def setMaxSize(funcName:String, jobPrio: JobPriority.JobPriority, size: Int ) = {
    jobPrio match {
      case JobPriority.High => highPrioFuncMaxSize += (funcName->size)
      case JobPriority.Low => lowPrioFuncMaxSize += (funcName->size)
      case _ => normalPrioFuncMaxSize += (funcName->size)
    }
  }
  
  def getMaxSize(funcName:String, jobPrio: JobPriority.JobPriority ): Int = {
    jobPrio match {
      case JobPriority.High => highPrioFuncMaxSize.getOrElse( funcName, 0 )
      case JobPriority.Low => lowPrioFuncMaxSize.getOrElse( funcName, 0 )
      case _ => normalPrioFuncMaxSize.getOrElse( funcName, 0 )
    }
  }
  
  def tryToAddJob( job: Job ): Boolean = {
  
    job.priority match {
      case JobPriority.High => addJobToQueue( job, highPrioFuncMaxSize, highPrioJobSize )
      case JobPriority.Low => addJobToQueue( job, lowPrioFuncMaxSize, lowPrioJobSize )
      case _ => addJobToQueue( job, normalPrioFuncMaxSize, normalPrioJobSize )
    }
  }
  
  def tryToRemoveJob( job: Job ) = {
    job.priority match {
      case JobPriority.High => removeJob( job, highPrioJobSize )
      case JobPriority.Low => removeJob( job, lowPrioJobSize )
      case _ => removeJob( job, normalPrioJobSize )
    }
  }
  
  private def addJobToQueue( job:Job, maxSizeQueue: Map[String, Int ], jobSizeQueue: Map[String, Int] ): Boolean = {
    val maxSize = maxSizeQueue.getOrElse( job.funcName, 0 )
    if( maxSize <= 0 ) return true
    
    val size = jobSizeQueue.getOrElse( job.funcName, 0 )
    if( size >= maxSize ) return false
    jobSizeQueue += (job.funcName -> ( size + 1 ) )
    return true
  }
  
  def removeJob( job: Job, jobSizeQueue: Map[String, Int] ) = {
    val size = jobSizeQueue.getOrElse( job.funcName, 0 )
    if( size > 0 ) {
       jobSizeQueue += ( job.funcName -> (size - 1) )
    }
  }
}                                        
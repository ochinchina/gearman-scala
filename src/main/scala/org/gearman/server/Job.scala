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

import org.gearman.message._
import org.gearman.channel._

/**
 * presents a received job from the client and also job status will be saved
 * to the job object
 * 
 * @param funcName function name of job
 * @param jobHandle the assigned unique job handle by the job server
 * @param uniqId the unique job identifier assigned by client
 * @param data the job data from client
 * @param priority the job priority(depends on the gearman message)
 * @param background true if the job is a background job
 * @param from the message channel(presents a client) which this job received from
 * @param processing the processing message channel(presents a worker)
 * @param numerator the worker reported completed percentage of numerator
 * @param denominator the worker reported completed percentage of denominator              
 */
case class Job( funcName: String,
                jobHandle: String,
                uniqId: String,
                data: String,
                reducer: Option[String],
                priority: JobPriority.JobPriority,
                background: Boolean,
                from: MessageChannel,
                var processing: MessageChannel = null,
                var numerator: Int = 0,
                var denominator: Int = 0  ) {

  private var datas_ = List[String]()

  /**
   * add the received data from client on this job
   *
   * @param data
   */
  def addData( data: String ) {
    datas_ = datas_ :+ data 
  }

  def sendDatas( channel: MessageChannel ) {
    while( datas_.nonEmpty ) {
      channel.send( WorkDataReq( jobHandle, datas_.head ) )
      datas_ = datas_.tail
    }
  }
}



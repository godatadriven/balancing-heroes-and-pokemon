/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.godatadriven.buzzwords.common

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scalaj.http.Http

case class FlinkOverviewResponse(running: List[FlinkJob],
                                 finished: List[FlinkJob])

case class FlinkJob(jid: String,
                    name: String,
                    state: String,
                    `start-time`: Long,
                    `end-time`: Long,
                    duration: Long,
                    `last-modification`: Long,
                    tasks: FlinkTasks)

case class FlinkTasks(total: Int,
                      pending: Int,
                      running: Int,
                      finished: Int,
                      canceling: Int,
                      canceled: Int,
                      failed: Int)

object FlinkControl {
  // This is a small class to get the running job from the jobmanager, this is required because on the executors
  // it is not possible to fetch the job id from the driver

  implicit val formats = org.json4s.DefaultFormats

  def getRunningJobs: List[String] = {
    val json = Http("http://localhost:8081/joboverview").asString.body
    val result = parse(json).extract[FlinkOverviewResponse]

    result.running.map(_.jid)
  }
}

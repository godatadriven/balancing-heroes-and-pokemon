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

package com.godatadriven.buzzwords.operators

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration

abstract class StatefulPlayerOperation[IN, OUT] extends RichMapFunction[IN, OUT] {

  // The key that defines the state, this one should be unique, unless you want to reuse the state somewhere else
  protected val STATE_KEY = "playerSkillDistribution"

  // Use ValueState:
  // All data stream functions can use managed state, but the raw state interfaces can only be used when implementing
  // operators. Using managed state (rather than raw state) is recommended, since with managed state Flink is able to
  // automatically redistribute state when the parallelism is changed, and also do better memory management.
  protected var playerSkill: ValueState[Array[Double]] = _

  // When opening the function, we want to initialize the shared state
  override def open(parameters: Configuration): Unit = {
    val descriptor = new ValueStateDescriptor[Array[Double]](
      STATE_KEY,
      createTypeInformation[Array[Double]]
    )
    playerSkill = getRuntimeContext.getState(descriptor)
  }
}

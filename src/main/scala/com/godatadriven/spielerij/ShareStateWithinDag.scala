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

package com.godatadriven.spielerij

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector


case class Message(keyField: String, count: Option[Int])

// Small piece of code to get an idea of the shared state capabilities of Flink

class MessageCounter extends ProcessFunction[Message, Message] {

  // Use ValueState:
  // All datastream functions can use managed state, but the raw state interfaces can only be used when implementing
  // operators. Using managed state (rather than raw state) is recommended, since with managed state Flink is able to
  // automatically redistribute state when the parallelism is changed, and also do better memory management.
  protected var messageCounter: ValueState[Integer] = _

  override def open(parameters: Configuration): Unit = {
    messageCounter = getRuntimeContext.getState(
      new ValueStateDescriptor[Integer]("messageCounter", createTypeInformation[Integer])
    )
  }

  override def processElement(msg: Message, ctx: ProcessFunction[Message, Message]#Context, out: Collector[Message]): Unit = {
    // Get the counter
    var count = messageCounter.value()

    // If we haven't seen the value before, set it to zero
    if (count == null) {
      count = 0
    }

    // Increment the state
    messageCounter.update(count + 1)

    // Set the count field of the message
    out.collect(Message(msg.keyField, Some(count)))
  }
}

object ShareStateWithinDag extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // This is a small Flink dag to become familiar with the concept of shared queryable state
  // Queryable state gives us the possibility to query state that is kept somewhere else at the dag
  // https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/stream/queryable_state.html

  // Useful url:
  // https://github.com/dataArtisans/flink-training-exercises/blob/master/src/main/scala/com/dataartisans/
  // flinktraining/exercises/datastream_scala/lowlatencyjoin/EventTimeJoinHelper.scala

  // TODO: Look at ReducingState for applying in balancing the heroes, the update function is actually some kind of reduce function

  env.fromCollection(List(
    Message("Gurbe", None),
    Message("Piebe", None),
    Message("Gurbe", None)
  )).keyBy(_.keyField)
    .process[Message](new MessageCounter()).print()

  env.execute("SocketHeroesStreaming")
}

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

package com.godatadriven.buzzwords

import com.godatadriven.buzzwords.common.JsonUtil
import com.godatadriven.buzzwords.definitions.Player
import com.godatadriven.buzzwords.operators._
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.scala._

object SocketHeroesStreaming {
  val hostName = "localhost"
  val port = 1925

  private val keySerializer = createTypeInformation[Player].createSerializer(new ExecutionConfig)

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    start(env.socketTextStream(hostName, port)).print()

    env.execute("SocketHeroesStreaming")
  }

  def start(stream: DataStream[String]): DataStream[String] = stream

    // Parse the JSON to a case class
    .map(line => JsonUtil.parseJson[Player](line))

    // Take the player as a key
    .keyBy(_.player)

    // Sample the historical skill of the player, if available
    .map(new SamplePlayerSkill)

    // Take the bucket as the key
    .keyBy(_._1)

    // Wait until there are ten players in the bucket
    .countWindow(Parameters.teamSize)

    // Determine the two teams of five players
    .apply(new DetermineTeam)

    // Play the actual game, this irl this won't be part of the pipeline
    .map(new PlayGame)

    // Map the game into player
    .flatMap(new ComputeNewPlayerSkill)

    .keyBy(_._1.player)

    .map(new UpdatePlayerSkill)
}

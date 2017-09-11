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

package com.godatadriven.buzzwords.sources

import com.godatadriven.buzzwords.Heroes
import com.godatadriven.buzzwords.common.{JsonUtil, LocalConfig}
import com.godatadriven.buzzwords.definitions.Player
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random


class PlayerGenerator extends SourceFunction[String] {
  private val rand = new Random

  override def cancel(): Unit = {

  }

  // Add some sleeping, because the generator is so fast, it will clog up all the queue's
  // In a real life situation the source will not be as fast as here
  private val sleep = 1000

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val heroes = Heroes.heroes.keys.size

    val playerIds = (0 until LocalConfig.numPlayer).toList

    // Simulate 500 games
    for (_ <- 0 until 500) {
      val playerIdsShuffled = util.Random.shuffle(playerIds)

      for (playerId <- playerIdsShuffled) {
        val randomIndex = rand.nextInt(heroes)
        val randomHero = Heroes.heroes.keys.toList(randomIndex)

        ctx.collect(JsonUtil.toJson(Player(playerId, randomHero)))
      }

      Thread.sleep(sleep)
    }
  }
}

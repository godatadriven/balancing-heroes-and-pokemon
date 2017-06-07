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

import com.godatadriven.buzzwords.definitions.{Game, UpdateStep}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class ComputeNewPlayerSkill extends FlatMapFunction[Game, UpdateStep] {
  override def flatMap(game: Game, out: Collector[UpdateStep]): Unit = {
    // Cut the game up in players and emit them
    for (player <- game.winning._1) {
      out.collect(UpdateStep(player, won = true, game.losing._2, game.queueBucket))
    }
    for (player <- game.losing._1) {
      out.collect(UpdateStep(player, won = false, game.winning._2, game.queueBucket))
    }
  }
}

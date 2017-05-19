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

import com.godatadriven.buzzwords.{Heroes, Parameters}
import com.godatadriven.buzzwords.definitions.{Player, Team}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import breeze.linalg._

class DetermineTeam extends WindowFunction[(Int, Player, Array[Double]), Team, Int, GlobalWindow] {
  private val emptyDist = DenseVector.zeros[Double](Parameters.skillDistributionBuckets)

  private def computeTeamDistribution(rawTeam: List[((Int, Player, Array[Double]), Int)]): (Set[Player], DenseVector[Double]) =
    rawTeam.map {
      case ((playerId: Int, playerCharacter: Player, playerSkill: Array[Double]), _: Int) => (playerCharacter, new DenseVector(playerSkill))
    }.foldLeft((Set[Player](), emptyDist))((team, player) => (team._1 + player._1, team._2 + (team._2 /:/ rawTeam.size.toDouble)))


  override def apply(key: Int, window: GlobalWindow, players: Iterable[(Int, Player, Array[Double])], out: Collector[Team]): Unit = {

    // Sort the team by the type of character, so we get mixed teams
    val teams = players
      .toList
      // Sort by the type of character, to get a balanced team
      .sortBy(player => Heroes.heroes(player._2.character))
      .zipWithIndex
      .groupBy(_._2 % 2 == 0)

    val firstTeam = computeTeamDistribution(teams(true))
    val secondTeam = computeTeamDistribution(teams(false))

    out.collect(Team(firstTeam, secondTeam))
  }
}

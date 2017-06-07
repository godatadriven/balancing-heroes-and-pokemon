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

import breeze.linalg._
import com.godatadriven.buzzwords.Heroes
import com.godatadriven.buzzwords.common.LocalConfig
import com.godatadriven.buzzwords.definitions.{Player, Team}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

object DetermineTeam {
  private val emptyDist = DenseVector.zeros[Double](LocalConfig.skillDistributionBuckets)

  def computeTeamDistribution(rawTeam: List[(Player, DenseVector[Double])]): (Set[Player], DenseVector[Double]) =
    rawTeam.foldLeft((Set[Player](), emptyDist))(
      (team, player) => (team._1 + player._1, team._2 + (player._2 /:/ rawTeam.size.toDouble))
    )
}

class DetermineTeam extends WindowFunction[(Int, Player, Array[Double]), Team, Int, GlobalWindow] {

  import DetermineTeam._

  override def apply(queueBucket: Int, window: GlobalWindow, players: Iterable[(Int, Player, Array[Double])], out: Collector[Team]): Unit = {

    // Sort the team by the type of character, so we get mixed teams
    val teams = players
      .toList
      // Sort by the type of character, to get a balanced team
      .sortBy(player => Heroes.heroes(player._2.character))
      .zipWithIndex
      .groupBy(_._2 % 2 == 0)
      // Map so we only take the essentials we need and get rid of all the indexes etc
      .mapValues(_.map(player => (player._1._2, new DenseVector[Double](player._1._3))))

    val firstTeam = computeTeamDistribution(teams(true))
    val secondTeam = computeTeamDistribution(teams(false))

    out.collect(Team(firstTeam, secondTeam, queueBucket))
  }
}

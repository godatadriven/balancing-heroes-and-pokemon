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

import com.godatadriven.buzzwords.common.GeneralLogger
import com.godatadriven.buzzwords.definitions.{Game, Player, Team}
import org.apache.flink.api.common.functions.MapFunction


object PlayGame {

  def determineScoreAvg(players: Set[Player]): Double =
    players.map(player => (player.id.toDouble % 100.0) / 100.0).sum

  def determineScoreMedian(players: Set[Player]): Double =
    players.toArray.sortBy(_.id).apply(players.size / 2).id.toDouble

  def determineWinner(firstTeam: Set[Player], secondTeam: Set[Player]): Boolean =
    determineScoreAvg(firstTeam) >= determineScoreAvg(secondTeam)

  def determineWinnerMedian(firstTeam: Set[Player], secondTeam: Set[Player]): Boolean =
    determineScoreMedian(firstTeam) >= determineScoreMedian(secondTeam)

  def determineWinnerPairwise(firstTeam: Set[Player], secondTeam: Set[Player]): Boolean =
    firstTeam
      .toList
      .sortBy(_.id)
      .zip(secondTeam.toList.sortBy(_.id))
      .map(playerPair => if (playerPair._1.id >= playerPair._2.id) 1 else -1).sum >= 0

  def playGame(team: Team): Game = {
    //val flip = Math.random() < (scoreA / (scoreA + scoreB))
    val flip = determineWinner(team.firstTeam._1, team.secondTeam._1)

    if (flip) {
      GeneralLogger.log(s"/tmp/player-stats-${team.firstTeam._1.head.id}.csv", "1")
      GeneralLogger.log(s"/tmp/player-stats-${team.secondTeam._1.head.id}.csv", "-1")
    } else {
      GeneralLogger.log(s"/tmp/player-stats-${team.firstTeam._1.head.id}.csv", "-1")
      GeneralLogger.log(s"/tmp/player-stats-${team.secondTeam._1.head.id}.csv", "1")
    }

    Game(
      // Winning team
      if (flip) team.firstTeam else team.secondTeam,

      // Losing team
      if (flip) team.secondTeam else team.firstTeam,

      // Keep track of the bucket
      team.queueBucket
    )
  }
}

// This function simulates the outcome of the game, irl this would be an actual game
class PlayGame extends MapFunction[Team, Game] {

  import PlayGame._

  override def map(team: Team): Game = playGame(team)
}

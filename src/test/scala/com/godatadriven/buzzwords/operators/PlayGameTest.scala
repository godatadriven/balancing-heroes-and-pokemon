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

import com.godatadriven.buzzwords.definitions.{Player, Team}
import org.scalatest.FlatSpec

class PlayGameTest extends FlatSpec {

  import PlayGame._

  "After playing a game" should "the team with the best players should win" in {

    val initDist = SamplePlayerSkill.initSkillDistributionBuckets

    val losingTeam = Set[Player](
      // Assassin
      Player(10001, "Alarak"),
      Player(10002, "Cassia"),

      // Specialists
      Player(10003, "Azmodan"),

      // Support
      Player(10004, "Brightwing"),

      // Warriors
      Player(10005, "Arthas")
    )

    val winningTeam = Set[Player](
      // Assassin
      Player(10011, "Chromie"),
      Player(10012, "Falstad"),

      // Specialists
      Player(10013, "Abathur"),

      // Support
      Player(10014, "Auriel"),

      // Warriors
      Player(10015, "Artanis")
    )

    val team = Team(
      firstTeam = (losingTeam, initDist),
      secondTeam = (winningTeam, initDist),
      queueBucket = 0
    )

    val game = playGame(team)

    assert(game.winning._1 == winningTeam)
    assert(game.losing._1 == losingTeam)
  }
}
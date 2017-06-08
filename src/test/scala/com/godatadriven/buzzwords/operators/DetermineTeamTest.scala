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

import breeze.linalg.sum
import com.godatadriven.buzzwords.definitions.{Player, Team}
import org.apache.flink.api.common.functions.util.ListCollector
import org.scalactic.TolerantNumerics
import org.scalatest.FlatSpec

class DetermineTeamTest extends FlatSpec {

  val epsilon = 1e-2f
  private implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  import DetermineTeam._

  "After forming teams" should "have two teams with a team distribution" in {
    val listCollected = new java.util.LinkedList[Team]()
    val listCollector = new ListCollector[Team](listCollected)

    val initDist = SamplePlayerSkill.initSkillDistributionBuckets.toArray

    determineTeam(
      queueBucket = 0,
      window = null,
      players = List(
        // Assassins
        (10012, Player(10012, "Alarak"), initDist),
        (10123, Player(10123, "Cassia"), initDist),
        (10234, Player(10234, "Chromie"), initDist),
        (10345, Player(10345, "Falstad"), initDist),

        // Specialists
        (10456, Player(10456, "Abathur"), initDist),
        (10567, Player(10567, "Azmodan"), initDist),

        // Support
        (10678, Player(10678, "Auriel"), initDist),
        (10789, Player(10789, "Brightwing"), initDist),

        // Warriors
        (10890, Player(10890, "Artanis"), initDist),
        (10901, Player(10901, "Arthas"), initDist)
      ),
      out = listCollector
    )

    // We should have one team
    assert(listCollected.size() == 1)

    val team = listCollected.getFirst

    // Should be normalized
    assert(sum(team.firstTeam._2) === 1.0)
    assert(sum(team.secondTeam._2) === 1.0)
  }
}

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

import com.godatadriven.buzzwords.Parameters
import com.godatadriven.buzzwords.definitions.Player

object SamplePlayerSkill {

  // Just create an uniform distribution as a starting point :-)
  def initSkillDistributionBuckets: Array[Double] = {
    val initValue = 1.0 / Parameters.skillDistributionBuckets
    (0 until Parameters.skillDistributionBuckets).map(_ => initValue).toArray
  }

  // Determine the highest bucket of an array of doubles
  def determineHighestBucket(dist: Array[Double]): Int =
    dist
      .zipWithIndex
      .foldLeft((0.0, 0))(
        (a, b) =>
          // Check if the new one is newer
          if (a._1 < b._1) b else a
      )._2 // Take the index of the bucket

}

class SamplePlayerSkill extends StatefulPlayerOperation[Player, (Int, Player, Array[Double])] {

  import SamplePlayerSkill._

  override def map(player: Player): (Int, Player, Array[Double]) = {

    var historicalSkillDistribution = playerSkill.value()

    // Check if the player has a skill distribution
    val result = if (historicalSkillDistribution == null) {
      historicalSkillDistribution = initSkillDistributionBuckets

      // Set so whe have a value in the db
      playerSkill.update(historicalSkillDistribution)

      // If the player doesn't have any history, just set him as average
      (Parameters.queueBuckets / 2, player, historicalSkillDistribution)
    } else {

      // Check which bucket is the highest
      val highestBucket = determineHighestBucket(historicalSkillDistribution)

      // Scale number of skill buckets to queue buckets
      val ration = Parameters.skillDistributionBuckets / Parameters.queueBuckets
      val queueBucket = highestBucket * ration

      (queueBucket, player, historicalSkillDistribution)
    }

    result
  }
}

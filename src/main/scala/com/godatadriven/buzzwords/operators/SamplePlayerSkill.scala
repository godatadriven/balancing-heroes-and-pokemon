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

import breeze.linalg.{DenseVector, sum}
import com.godatadriven.buzzwords.common.{FlinkControl, FlinkSharedStateQueryClient, LocalConfig}
import com.godatadriven.buzzwords.definitions.Player
import org.apache.flink.api.common.functions.MapFunction
import org.slf4j.LoggerFactory

import scala.util.Random

object SamplePlayerSkill {

  def initSkillDistributionBuckets: DenseVector[Double] = {
    val vec = DenseVector.ones[Double](LocalConfig.skillDistributionBuckets)

    // Create a bump in the middle so it will pick at as a starting point
    vec(LocalConfig.skillDistributionBuckets / 2) += 0.1

    // Normalize
    vec /:/ sum(vec)
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

  def mapSkillBucketToQueue(skillBucket: Int): Int =
    (skillBucket * (LocalConfig.queueBuckets.toDouble / LocalConfig.skillDistributionBuckets.toDouble)).toInt
}

class SamplePlayerSkill extends MapFunction[Player, (Int, Player, Array[Double])] {
  private val logger = LoggerFactory.getLogger(getClass)

  import SamplePlayerSkill._

  private lazy val stateClient = {
    val runningJobs = FlinkControl.getRunningJobs

    if (runningJobs.nonEmpty) {
      Some(new FlinkSharedStateQueryClient(runningJobs.head))
    } else {
      None
    }
  }

  override def map(player: Player): (Int, Player, Array[Double]) = {
    val rnd = new Random()

    val dist = stateClient match {
      case Some(client) => client.executeQuery(LocalConfig.keyStateName, player.id).getOrElse(
        // If the user hasn't played yet, just take the default distribution
        initSkillDistributionBuckets.toArray
      )

      case None => {
        logger.warn("The shared state client is not available, are you running on a cluster?")

        // If the client not available, take the default distribution
        initSkillDistributionBuckets.toArray
      }
    }

    // Check which bucket is the highest
    val highestBucket = determineHighestBucket(dist)

    // Add some randomness (either -1, 0, +1) and keep within the bounds
    val randomizedSkillBucket = Math.max(Math.min(highestBucket + (rnd.nextInt(3) - 1), LocalConfig.skillDistributionBuckets - 1), 0)

    // Scale number of skill buckets to queue buckets
    val queueBucket = mapSkillBucketToQueue(randomizedSkillBucket)

    (queueBucket, player, dist)
  }
}

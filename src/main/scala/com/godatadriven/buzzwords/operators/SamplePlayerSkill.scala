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
import breeze.stats.distributions.Beta
import breeze.stats.hist
import com.godatadriven.buzzwords.common.{FlinkControl, FlinkSharedStateQueryClient, LocalConfig}
import com.godatadriven.buzzwords.definitions.Player
import org.apache.flink.api.common.functions.MapFunction
import org.slf4j.LoggerFactory

import scala.util.Random

object SamplePlayerSkill {

  private val g = breeze.stats.distributions.Gaussian(0, 1)
  private val SAMPLES = 10000000

  def initSkillDistributionBuckets: DenseVector[Double] = initSkillDistributionBucketsGuass

  def initSkillDistributionBucketsGuass: DenseVector[Double] = {
    val vec = hist(g.sample(SAMPLES), LocalConfig.skillDistributionBuckets).hist

    // Make a small bump in the center, so we start always in the center
    vec(LocalConfig.skillDistributionBuckets / 2) += 1.0 / vec.length

    // Normalize
    vec /:/ sum(vec)
  }

  def initSkillDistributionBucketsBeta: DenseVector[Double] = {
    val rnd = new Random(System.currentTimeMillis())

    val beta = new Beta(1.0 + (rnd.nextDouble() * 1.5), 1.0 + (rnd.nextDouble() * 1.5))

    val vec = DenseVector[Double](
      (0 until LocalConfig.skillDistributionBuckets)
        .map(_.toDouble / LocalConfig.skillDistributionBuckets)
        .map(number => beta.dist(number)).toArray
    )

    // Normalize
    vec /:/ sum(vec)
  }

  def initSkillDistributionBucketsFlat: DenseVector[Double] = {
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
      .maxBy(_._1)._2 // Take the index of the bucket

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

    // Scale number of skill buckets to queue buckets
    val queueBucket = mapSkillBucketToQueue(highestBucket)

    val finalBucket = Math.max(Math.min(queueBucket + (rnd.nextInt(3) - 1), LocalConfig.queueBuckets), 0)

    (finalBucket, player, dist)
  }
}

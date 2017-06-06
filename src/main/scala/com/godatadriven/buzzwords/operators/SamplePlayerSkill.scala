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
import breeze.stats._
import breeze.stats.distributions.Gaussian
import com.godatadriven.buzzwords.Parameters
import com.godatadriven.buzzwords.common.{FlinkControl, FlinkSharedStateQueryClient, LocalConfig}
import com.godatadriven.buzzwords.definitions.Player
import org.apache.flink.api.common.functions.MapFunction

object SamplePlayerSkill {
  private val numberOfSamples = 220000
  private val startDistributionMean = 0.5
  private val startDistributionVar = 0.3


  /*  // Just create an uniform distribution as a starting point :-)
    def initSkillDistributionBuckets: DenseVector[Double] = {
      // Sample a gaussian function to obtain a normal distribution
      val guassian = Gaussian(startDistributionMean, startDistributionVar)
      val initDistribution = hist(guassian.sample(numberOfSamples), Parameters.skillDistributionBuckets).hist

      val totalSum = sum(initDistribution)

      initDistribution /:/ totalSum
    }*/

  def initSkillDistributionBuckets: DenseVector[Double] = {
    val vec = DenseVector.ones[Double](Parameters.skillDistributionBuckets)

    // Create a bump in the middle so it will pick at as a starting point
    vec(Parameters.skillDistributionBuckets / 2) += 0.1

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
    (skillBucket * (Parameters.queueBuckets.toDouble / Parameters.skillDistributionBuckets.toDouble)).toInt
}

class SamplePlayerSkill extends MapFunction[Player, (Int, Player, Array[Double])] {

  import SamplePlayerSkill._

  lazy val stateClient = new FlinkSharedStateQueryClient(
    // TODO: Make some exception handling in case there is no head
    FlinkControl.getRunningJobs.head
  )

  override def map(player: Player): (Int, Player, Array[Double]) = {

    // Check if the player did a game before and we know his skill
    val dist = stateClient.executeQuery(LocalConfig.keyStateName, player.id).getOrElse(
      // If there is no skill available, just take a normal distribution
      initSkillDistributionBuckets.toArray
    )

    // Check which bucket is the highest
    val highestBucket = determineHighestBucket(dist)

    // Scale number of skill buckets to queue buckets
    val queueBucket = mapSkillBucketToQueue(highestBucket)

    (queueBucket, player, dist)
  }
}

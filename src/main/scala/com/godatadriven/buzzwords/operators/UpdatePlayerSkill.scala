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
import breeze.stats._
import com.godatadriven.buzzwords.common.LocalConfig
import com.godatadriven.buzzwords.definitions.UpdateStep
import org.apache.flink.api.common.functions.FoldFunction

object UpdatePlayerSkill {

  def getPrior(loser: DenseVector[Double], winner: DenseVector[Double]): DenseMatrix[Double] =
    loser.toDenseMatrix.t * winner.toDenseMatrix

  def cutMatrix(mat: DenseMatrix[Double]): DenseMatrix[Double] = {
    val posterior = upperTriangular(mat) + 0.000001
    posterior /:/ sum(posterior)
  }

  def getMarginals(posteriorMat: DenseMatrix[Double]): (DenseVector[Double], DenseVector[Double]) = {
    val winner = sum(posteriorMat, Axis._0).t
    val losing = sum(posteriorMat, Axis._1)

    (winner /:/ sum(winner), losing /:/ sum(losing))
  }
}

//val highestBucketOpponent = SamplePlayerSkill.determineHighestBucket(value.opponentDistribution.toArray)
//val ratio = LocalConfig.skillDistributionBuckets.toDouble / LocalConfig.skillDistributionBuckets.toDouble
//  val centerBucketQueue = (value.queuebucker * ratio) + (ratio / 2.0)

class UpdatePlayerSkill extends FoldFunction[UpdateStep, Array[Double]] {
  private val SAMPLES = 10000000

  import UpdatePlayerSkill._

  override def fold(prevPlayerSkillAcc: Array[Double], value: UpdateStep): Array[Double] = {
    val prevPlayerSkill = DenseVector[Double](prevPlayerSkillAcc)

    // Generate a normal distribution, half width of total distribution
    val bucketDist = DenseVector.ones[Double](LocalConfig.skillDistributionBuckets)
    val g = breeze.stats.distributions.Gaussian(0, 1)
    val normalDist = hist(g.sample(SAMPLES), LocalConfig.skillDistributionBuckets / 2).hist

    // Find the centre skill of the other team
    val opponentSkillCenter = SamplePlayerSkill.determineHighestBucket(value.opponentDistribution.toArray)

    var pos = 0
    for (idx <- 0 until LocalConfig.skillDistributionBuckets) {
      if (idx >= (opponentSkillCenter - (normalDist.length / 2)) && idx <= (opponentSkillCenter + (normalDist.length / 2))) {
        bucketDist(idx) = normalDist(pos)
        pos = pos + 1
      }
    }

    // Normalise it
    bucketDist /:/ sum(bucketDist)
    // Build the prior matrix based on the game
    val matPrior = if (value.won) {
      getPrior(bucketDist, prevPlayerSkill)
    } else {
      getPrior(prevPlayerSkill, bucketDist)
    }

    val matPosterior = cutMatrix(matPrior)
    val margins = getMarginals(matPosterior)

    // Check if won
    val nextPlayerSkill = if (value.won) {
      margins._1
    } else {
      margins._2
    }

    val prevBucket = SamplePlayerSkill.determineHighestBucket(prevPlayerSkill.toArray)
    val nextBucket = SamplePlayerSkill.determineHighestBucket(nextPlayerSkill.toArray)

    val log = if (value.won) {
      s"Player #${value.player.id} has won, and went from bucket $prevBucket to $nextBucket"
    } else {
      s"Player #${value.player.id} has lost, and went from bucket $prevBucket to $nextBucket"
    }
    import java.io._
    val bw2 = new FileWriter(new File(s"/tmp/player-update-${value.player.id}.csv"), true)
    bw2.write(bucketDist.toArray.mkString(",") + "\n")
    bw2.close()
    import java.io._
    val bw = new FileWriter(new File(s"/tmp/player-${value.player.id}.csv"), true)
    bw.write(prevPlayerSkill.toArray.mkString(",") + "\n")
    bw.close()

    nextPlayerSkill.toArray
  }
}

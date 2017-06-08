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
import com.godatadriven.buzzwords.common.{GeneralLogger, LocalConfig}
import com.godatadriven.buzzwords.definitions.UpdateStep
import org.apache.flink.api.common.functions.FoldFunction

object UpdatePlayerSkill {
  private val g = breeze.stats.distributions.Gaussian(0, 1)
  private val SAMPLES = 1000000
  private val gaus = hist(g.sample(SAMPLES), LocalConfig.skillDistributionBuckets).hist

  private def createOpponentDistribution(dist: DenseVector[Double]): DenseVector[Double] = {
    val vectorWidth = dist.length

    // From the distribution, get the highest bucket
    val opponentSkillCenter = SamplePlayerSkill.determineHighestBucket(dist.toArray)

    // Create an empty distribution of ones
    val bucketDist = DenseVector.ones[Double](vectorWidth)

    // Bucket a gaussian half as wide as the total distribution
    val normalDist = gaus.copy

    val start = opponentSkillCenter - (normalDist.length / 2)
    val stop = start + normalDist.length
    var pos = 0
    for (idx <- start until stop) {
      if (idx >= 0 && idx < bucketDist.length) {
        bucketDist(idx) = normalDist(pos)
      }
      pos = pos + 1
    }

    bucketDist /:/ sum(bucketDist)
  }

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

class UpdatePlayerSkill extends FoldFunction[UpdateStep, Array[Double]] {

  import UpdatePlayerSkill._

  override def fold(prevPlayerSkillAcc: Array[Double], value: UpdateStep): Array[Double] = {
    val prevPlayerSkill = DenseVector[Double](prevPlayerSkillAcc)

    // val opponentDist = createOpponentDistribution(value.opponentDistribution)
    val opponentDist = value.opponentDistribution

    // Build the prior matrix based on the game
    val matPrior = if (value.won) {
      getPrior(opponentDist, prevPlayerSkill)
    } else {
      getPrior(prevPlayerSkill, opponentDist)
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

    GeneralLogger.log(s"/tmp/player-update-${value.player.id}.csv", opponentDist.toArray.mkString(","))
    GeneralLogger.log(s"/tmp/player-${value.player.id}.csv", prevPlayerSkill.toArray.mkString(","))

    nextPlayerSkill.toArray
  }
}

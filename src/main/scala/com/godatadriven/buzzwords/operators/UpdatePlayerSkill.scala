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

import com.godatadriven.buzzwords.definitions.UpdateStep
import org.apache.flink.api.common.functions.FoldFunction
import org.slf4j.LoggerFactory

import breeze.linalg._

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

class UpdatePlayerSkill extends FoldFunction[UpdateStep, Array[Double]] {
  private val logger = LoggerFactory.getLogger(getClass)

  import UpdatePlayerSkill._

  override def fold(prevPlayerSkillAcc: Array[Double], value: UpdateStep): Array[Double] = {

    val prevPlayerSkill = DenseVector[Double](prevPlayerSkillAcc)

    val matPrior = if (value.won) {
      getPrior(value.opponentDistribution, prevPlayerSkill)
    } else {
      getPrior(prevPlayerSkill, value.opponentDistribution)
    }

    val matPosterior = cutMatrix(matPrior)

    val margins = getMarginals(matPosterior)


    //logger.warn("Posterior: " + matPosterior.toArray.mkString(","))

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

    //  if (List(2, 20, 50, 70, 90).contains(value.player.id)) {
    println(log)
    println(prevPlayerSkillAcc.mkString(","))

    //}

    nextPlayerSkill.toArray
  }
}

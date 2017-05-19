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

import breeze.linalg.{*, DenseMatrix, DenseVector, sum, upperTriangular}
import com.godatadriven.buzzwords.definitions.Player


object UpdatePlayerSkill {
  def getPrior(arr1: DenseVector[Double], arr2: DenseVector[Double]): DenseMatrix[Double] =
    arr1.toDenseMatrix.t * arr2.toDenseMatrix

  def cutMatrix(mat: DenseMatrix[Double]): DenseMatrix[Double] = {
    val posterior = upperTriangular(mat) + 0.000001
    posterior /:/ sum(posterior)
  }

  def getMarginals(posteriorMat: DenseMatrix[Double]): DenseVector[Double] = sum(posteriorMat.t(*, ::))
}

class UpdatePlayerSkill extends StatefulPlayerOperation[(Player, Boolean, DenseVector[Double]), String] {

  import UpdatePlayerSkill._

  override def map(gameResult: (Player, Boolean, DenseVector[Double])): String = {

    var rawPlayerSkill = playerSkill.value()

    if (rawPlayerSkill == null) {
      rawPlayerSkill = SamplePlayerSkill.initSkillDistributionBuckets
    }

    val prevPlayerSkill = DenseVector(rawPlayerSkill)

    // Check if won
    val matPrior = if (gameResult._2) {
      getPrior(gameResult._3, prevPlayerSkill)
    } else {
      getPrior(prevPlayerSkill, gameResult._3)
    }

    val matPosterior = cutMatrix(matPrior)

    val nextPlayerSkill = getMarginals(matPosterior)

    playerSkill.update(nextPlayerSkill.toArray)

    val prevBucket = SamplePlayerSkill.determineHighestBucket(prevPlayerSkill.toArray)
    val nextBucket = SamplePlayerSkill.determineHighestBucket(nextPlayerSkill.toArray)

    if (gameResult._2) {
      s"Player #${gameResult._1.player} has won, and went from bucket $prevBucket to $nextBucket"
    } else {
      s"Player #${gameResult._1.player} has lost, and went from bucket $prevBucket to $nextBucket"
    }
  }
}

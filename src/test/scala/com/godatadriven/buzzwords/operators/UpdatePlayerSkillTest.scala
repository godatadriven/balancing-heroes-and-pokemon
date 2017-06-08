
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

import breeze.linalg.{DenseVector, _}
import org.scalactic.TolerantNumerics
import org.scalatest.FlatSpec

class UpdatePlayerSkillTest extends FlatSpec {

  val epsilon = 1e-2f
  private implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  import UpdatePlayerSkill._

  "The player skill update step" should "correctly update the player skill distribution" in {
    val playerOne = DenseVector[Double](1.0, 2.0, 3.0)
    val playerTwo = DenseVector[Double](3.0, 2.0, 1.0)
    val playerThree = DenseVector[Double](1.0, 1.0, 1.0)

    val matOne = getPrior(playerOne, playerTwo)

    assert(matOne == DenseMatrix(
      (3.0, 2.0, 1.0),
      (6.0, 4.0, 2.0),
      (9.0, 6.0, 3.0)
    ))

    val matTwo = getPrior(playerTwo, playerOne)

    assert(matTwo == DenseMatrix(
      (3.0, 6.0, 9.0),
      (2.0, 4.0, 6.0),
      (1.0, 2.0, 3.0)
    ))

    val matThree = getPrior(playerThree, playerThree)

    assert(matThree == DenseMatrix(
      (1.0, 1.0, 1.0),
      (1.0, 1.0, 1.0),
      (1.0, 1.0, 1.0)
    ))

    val marginalsOne = getMarginals(matOne)

    assert(marginalsOne == (
      DenseVector(0.5, 0.3333333333333333, 0.16666666666666666),
      DenseVector(0.16666666666666666, 0.3333333333333333, 0.5)
    ))

    // Should be normalized
    assert(sum(marginalsOne._1) === 1.0)
    assert(sum(marginalsOne._2) === 1.0)

    // The other way around
    val marginalsTwo = getMarginals(matTwo)

    assert(marginalsTwo == (
      DenseVector(0.16666666666666666, 0.3333333333333333, 0.5),
      DenseVector(0.5, 0.3333333333333333, 0.16666666666666666)
    ))

    // Should be normalized
    assert(sum(marginalsTwo._1) === 1.0)
    assert(sum(marginalsTwo._2) === 1.0)
  }

  "The generated distribution" should "conform to some specs" in {
    val vectorSize = 200
    val vectorHighestBucket = 22

    val distVector = DenseVector.zeros[Double](vectorSize)

    distVector(vectorHighestBucket) = 0.001 // Set a bump here

    val highestBucket = SamplePlayerSkill.determineHighestBucket(distVector.toArray)

    assert(highestBucket == vectorHighestBucket)
  }

  "After performing the player update step" should "the bucket should increase" in {
    val losingDistribution = SamplePlayerSkill.initSkillDistributionBuckets

    val playerPreviousIteration = SamplePlayerSkill.initSkillDistributionBuckets

    val matPrior = getPrior(playerPreviousIteration, losingDistribution)

    val matPosterior = cutMatrix(matPrior)

    // In case of win
    val nextPlayerSkill = getMarginals(matPosterior)._1

    val beforeWinning = SamplePlayerSkill.determineHighestBucket(playerPreviousIteration.toArray)
    val afterWinning = SamplePlayerSkill.determineHighestBucket(nextPlayerSkill.toArray)

    println(s"Went to: $beforeWinning -> $afterWinning")

    assert(beforeWinning < afterWinning)
  }

  "When constructing the matrix" should "be give the original vectors back" in {
    val vec1 = DenseVector(1.0, 2.0, 3.0, 4.0, 5.0)
    val vec2 = DenseVector(5.0, 6.0, 7.0, 8.0, 9.0)

    val normVec = vec1 /:/ sum(vec1)

    val mat = vec1.toDenseMatrix.t * vec2.toDenseMatrix

    assert(sum(mat(*, ::)) /:/ sum(sum(mat(*, ::))) == normVec)
  }

}


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

import breeze.linalg.{DenseMatrix, DenseVector}
import org.scalatest.FlatSpec

class UpdatePlayerSkillTest extends FlatSpec {

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

    assert(marginalsOne == DenseVector(6.0, 12.0, 18.0))

    // The other way around
    val marginalsTwo = getMarginals(matTwo)

    assert(marginalsTwo == DenseVector(18.0, 12.0, 6.0))
  }


  "After performing the player update step" should "the bucket should increase" in {
    val losing = DenseVector(SamplePlayerSkill.initSkillDistributionBuckets)

    val prevPlayerSkill = DenseVector(SamplePlayerSkill.initSkillDistributionBuckets)

    val matPrior = getPrior(losing, prevPlayerSkill)

    val matPosterior = cutMatrix(matPrior)

    val nextPlayerSkill = getMarginals(matPosterior)

    val prevBucket = SamplePlayerSkill.determineHighestBucket(prevPlayerSkill.toArray)
    val nextBucket = SamplePlayerSkill.determineHighestBucket(nextPlayerSkill.toArray)

    println(prevPlayerSkill)
    println(nextPlayerSkill)

    assert(prevBucket < nextBucket)
  }

}

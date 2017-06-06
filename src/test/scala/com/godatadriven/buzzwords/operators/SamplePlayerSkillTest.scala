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

import breeze.linalg.sum
import org.scalactic.TolerantNumerics
import org.scalatest.FlatSpec

class SamplePlayerSkillTest extends FlatSpec {

  val epsilon = 1e-2f
  private implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  import SamplePlayerSkill._

  "The sampling of the player" should "should pick the highest bucket" in {
    val buckets = Array(
      0.0,
      0.0,
      0.0,
      0.0,
      2.0,
      0.0,
      0.0,
      0.0
    )

    assert(determineHighestBucket(buckets) == 4)
  }

  "The default player distribution" should "be normalized to one" in {
    assert(sum(initSkillDistributionBuckets) === 1.0)
  }
}

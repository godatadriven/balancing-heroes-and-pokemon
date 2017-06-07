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
import com.godatadriven.buzzwords.common.LocalConfig
import org.scalactic.TolerantNumerics
import org.scalatest.FlatSpec

class SamplePlayerSkillTest extends FlatSpec {

  val epsilon = 1e-2f
  private implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  import SamplePlayerSkill._

  "The sampling of the player" should "pick the highest bucket" in {
    val dist = Array(
      .322772211553224E-5,
      2.4528958113356387E-5,
      2.7362938589641077E-5,
      3.3349273314738526E-5,
      4.956555586605835E-5,
      1.0276313654542473E-4,
      2.719396707925466E-4,
      7.471248843116837E-4,
      0.001916090998944159,
      0.004467765593018056,
      0.011311365854339634,
      0.01969260890846565,
      0.035028801117055676,
      0.057723758587935194,
      0.08817706570494492,
      0.12442220614150801,
      0.16058756490892978,
      0.1854787146737169,
      0.18195322041443804,
      0.127960974957055
    )
    assert(determineHighestBucket(dist) == 17)
  }

  "The initial distribution" should "start in the centre of the distribution" in {
    assert(determineHighestBucket(initSkillDistributionBuckets.toArray) == LocalConfig.skillDistributionBuckets / 2)
  }

  "The default player distribution" should "be normalized to one" in {
    assert(sum(initSkillDistributionBuckets) === 1.0)
  }
}

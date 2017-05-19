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

package com.godatadriven.buzzwords.common

import com.godatadriven.buzzwords.definitions.{Game, Player}
import org.scalatest.FlatSpec

class JsonUtilTest extends FlatSpec {

//  "The json parser" should "be able to parse the game" in {
//    val player = Player(1234567890, "diablo")
//
//    val gameJson =
//      """
//        |{
//        |	"winner": [{
//        |			"player": 1234567890,
//        |			"character": "diablo"
//        |		},
//        |		{
//        |			"player": 1234567891,
//        |			"character": "diablo"
//        |		},
//        |		{
//        |			"player": 1234567892,
//        |			"character": "diablo"
//        |		},
//        |		{
//        |			"player": 1234567890,
//        |			"character": "diablo"
//        |		},
//        |		{
//        |			"player": 1234567890,
//        |			"character": "diablo"
//        |		}
//        |	],
//        |	"loser": [{
//        |			"player": 1234567890,
//        |			"character": "diablo"
//        |		},
//        |		{
//        |			"player": 1234567890,
//        |			"character": "diablo"
//        |		},
//        |		{
//        |			"player": 1234567890,
//        |			"character": "diablo"
//        |		},
//        |		{
//        |			"player": 1234567890,
//        |			"character": "diablo"
//        |		},
//        |		{
//        |			"player": 1234567890,
//        |			"character": "diablo"
//        |		}
//        |	]
//        |}
//      """.stripMargin
//
//    val game: Game = JsonUtil.parseJson[Game](gameJson)
//
//    // Check if all players are diablo
//    assert(game.winning._1.forall(_ == player))
//    assert(game.losing._1.forall(_ == player))
//  }
}

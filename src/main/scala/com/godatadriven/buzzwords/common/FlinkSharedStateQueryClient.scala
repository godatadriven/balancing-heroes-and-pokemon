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

import java.util.concurrent.Executors

import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.{ExecutionConfig, JobID}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils
import org.apache.flink.runtime.query.QueryableStateClient
import org.apache.flink.runtime.query.netty.UnknownKeyOrNamespace
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer
import org.apache.flink.runtime.state.{VoidNamespace, VoidNamespaceSerializer}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, duration}

class FlinkSharedStateQueryClient(jobID: String) {
  implicit val typeInfo = TypeInformation.of(classOf[Long])
  implicit val keySerializer = createTypeInformation[Long].createSerializer(new ExecutionConfig)

  private val client = getQueryableStateClient()

  def executeQuery(keyStateName: String, key: Long): Option[Array[Double]] = {
    // Serialize request
    val seralizedKey = getSeralizedKey(key)

    // Query Flink state
    val future = client.getKvState(
      JobID.fromHexString(jobID),
      keyStateName,
      key.hashCode,
      seralizedKey
    )

    try {
      // Await async result
      val serializedResult: Array[Byte] = Await.result(
        future,
        new FiniteDuration(2, duration.SECONDS)
      )

      // Deserialize response
      val results = KvStateRequestSerializer.deserializeValue[Array[Double]](
        serializedResult,
        getValueSerializer
      )

      Some(results)
    } catch {
      // In case the key doesn't exist
      case _: UnknownKeyOrNamespace => None
    }
  }

  private def getQueryableStateClient(config: Configuration = LocalConfig.getFlinkConfig): QueryableStateClient = {
    val highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
      config,
      Executors.newSingleThreadScheduledExecutor,
      HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION
    )

    val client: QueryableStateClient = new QueryableStateClient(
      config,
      highAvailabilityServices
    )
    client
  }

  private def getValueSerializer: TypeSerializer[Array[Double]] =
    TypeInformation.of(new TypeHint[Array[Double]]() {}).createSerializer(new ExecutionConfig)


  private def getSeralizedKey(key: Long): Array[Byte] = {
    val serializedKey: Array[Byte] =
      KvStateRequestSerializer.serializeKeyAndNamespace(
        key,
        keySerializer,
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE
      )

    serializedKey
  }
}


/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.security.kubernetes

import akka.actor.ActorSystem

import scala.concurrent.duration.Duration
import scala.concurrent.Await

private class Server {

  private val actorSystem = ActorSystem("HadoopTokenRenewServer")
  private var secretFinder : SecretFinder = _

  def start(): Unit = {
    val renewService = TokenRenewService(actorSystem)
    secretFinder = SecretFinder(renewService)
  }

  def join() : Unit = {
    // scalastyle:off awaitready
    Await.ready(actorSystem.whenTerminated, Duration.Inf)
    // scalastyle:on awaitready
  }

  def stop(): Unit = {
    actorSystem.terminate()
    secretFinder.stop()
  }
}

object HadoopTokenRenewServer {

  def main(args: Array[String]): Unit = {
    val server = new Server
    server.start()
    try {
      server.join()
    } finally {
      server.stop()
    }
  }
}

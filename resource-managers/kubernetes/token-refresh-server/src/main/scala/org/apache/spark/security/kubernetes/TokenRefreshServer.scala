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

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import akka.actor.ActorSystem
import io.fabric8.kubernetes.client.DefaultKubernetesClient
import org.apache.log4j.{Level, Logger}

import scala.annotation.tailrec


private class Server {

  private val actorSystem = ActorSystem("TokenRefreshServer")
  private val kubernetesClient = new DefaultKubernetesClient
  private var secretFinder : Option[SecretFinder] = None

  def start(): Unit = {
    val renewService = TokenRefreshService(actorSystem, kubernetesClient)
    secretFinder = Some(SecretFinder(renewService, kubernetesClient))
  }

  def join() : Unit = {
    // scalastyle:off awaitready
    Await.ready(actorSystem.whenTerminated, Duration.Inf)
    // scalastyle:on awaitready
  }

  def stop(): Unit = {
    actorSystem.terminate()
    secretFinder.foreach(_.stop())
  }
}

/*
 * TODO: Support REST endpoint for checking status of tokens.
 */
object TokenRefreshServer {

  private class Arguments(args: List[String]) {

    var logLevel: Level = Level.WARN

    parse(args)

    @tailrec
    private def parse(args: List[String]): Unit = args match {
      case ("--verbose" | "-v") :: tail =>
        logLevel = Level.INFO
        parse(tail)
      case ("--debug" | "-d") :: tail =>
        logLevel = Level.DEBUG
        parse(tail)
      case _ =>
    }
  }

  def main(args: Array[String]): Unit = {
    val parsedArgs = new Arguments(args.toList)
    Logger.getRootLogger.setLevel(parsedArgs.logLevel)
    val server = new Server
    try {
      server.start()
      server.join()
    } finally {
      server.stop()
    }
  }
}

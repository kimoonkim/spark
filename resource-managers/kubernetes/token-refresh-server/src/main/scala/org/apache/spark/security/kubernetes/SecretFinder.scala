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

import java.lang
import java.util.{Timer, TimerTask}

import scala.collection.JavaConverters._

import akka.actor.ActorRef
import io.fabric8.kubernetes.api.model.{Secret, SecretList}
import io.fabric8.kubernetes.client._
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable

import org.apache.spark.security.kubernetes.constants._

private class SecretFinder(renewService: ActorRef,
                           timer: Timer,
                           kubernetesClient: KubernetesClient) {

  timer.schedule(new SecretScanner(renewService, kubernetesClient),
    SECRET_SCANNER_INITIAL_DELAY_MILLIS, SECRET_SCANNER_PERIOD_MILLIS)
  SecretFinder.selectSecrets(kubernetesClient)
    .watch(new SecretWatcher(renewService))

  def stop(): Unit = {
    timer.cancel()
    kubernetesClient.close()
  }
}

private class SecretScanner(renewService: ActorRef,
                            kubernetesClient: KubernetesClient) extends TimerTask with Logging {

  override def run(): Unit = {
    val secrets = SecretFinder.selectSecrets(kubernetesClient).list.getItems.asScala.toList
    logInfo(s"Scanned ${secrets.map(_.getMetadata.getSelfLink).mkString}")
    renewService ! UpdateSecretsToTrack(secrets)
  }
}

private class SecretWatcher(renewService: ActorRef) extends Watcher[Secret] with Logging {

  override def eventReceived(action: Action, secret: Secret): Unit = {
    action match {
      case Action.ADDED =>
        logInfo(s"Found ${secret.getMetadata.getSelfLink} added")
        renewService ! StartRefresh(secret)
      case Action.DELETED =>
        logInfo(s"Found ${secret.getMetadata.getSelfLink} deleted")
        renewService ! StopRefresh(secret)
      case _ =>  // Do nothing
    }
  }

  override def onClose(cause: KubernetesClientException): Unit = {
    // Do nothing
  }
}

private object SecretFinder {

  def apply(renewService: ActorRef, kubernetesClient: KubernetesClient) : SecretFinder = {
    new SecretFinder(renewService,
      new Timer(SECRET_SCANNER_THREAD_NAME, IS_DAEMON_THREAD),
      kubernetesClient)
  }

  def selectSecrets(kubernetesClient: KubernetesClient):
          FilterWatchListDeletable[Secret, SecretList, lang.Boolean, Watch, Watcher[Secret]] = {
    kubernetesClient
      .secrets()
      .inAnyNamespace()
      .withLabel(SECRET_LABEL_KEY_REFRESH_HADOOP_TOKENS, SECRET_LABEL_VALUE_REFRESH_HADOOP_TOKENS)
  }
}

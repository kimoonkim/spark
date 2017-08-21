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

import java.util.{Timer, TimerTask}

import scala.collection.JavaConverters._

import akka.actor.ActorRef
import io.fabric8.kubernetes.api.model.Secret
import io.fabric8.kubernetes.client.{DefaultKubernetesClient, KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action

import org.apache.spark.security.kubernetes.constants._

private class SecretFinder(renewService: ActorRef,
                           timer: Timer,
                           kubernetesClient: KubernetesClient) {

  timer.schedule(new SecretScanner(renewService, kubernetesClient), 1000L, 1000L)
  kubernetesClient
    .secrets()
    .withLabel(HADOOP_DELEGATION_TOKEN_LABEL_IN_SECRET)
    .watch(new SecretWatcher(renewService))

  def stop(): Unit = {
    timer.cancel()
    kubernetesClient.close()
  }
}

private class SecretScanner(renewService: ActorRef,
                            kubernetesClient: KubernetesClient) extends TimerTask {

  override def run(): Unit = {
    val secrets = kubernetesClient
      .secrets
      .withLabel(HADOOP_DELEGATION_TOKEN_LABEL_IN_SECRET)
    renewService ! UpdateRefreshList(secrets.list.getItems.asScala.toList)
  }
}

private class SecretWatcher(renewService: ActorRef) extends Watcher[Secret] {

  override def eventReceived(action: Action, secret: Secret): Unit = {
    action match {
      case Action.ADDED =>
        renewService ! StartRefresh(secret)
      case Action.DELETED =>
        renewService ! StopRefresh(secret)
    }
  }

  override def onClose(cause: KubernetesClientException): Unit = {
    // FIXME. TBD.
  }
}

private object SecretFinder {

  def apply(renewService: ActorRef) : SecretFinder = {
    new SecretFinder(renewService,
      new Timer(SECRET_FIND_THREAD_NAME, IS_DAEMON_THREAD),
      new DefaultKubernetesClient)
  }
}

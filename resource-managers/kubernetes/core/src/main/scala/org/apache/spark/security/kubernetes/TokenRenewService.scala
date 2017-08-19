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

import java.io.{ByteArrayInputStream, DataInputStream}
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import io.fabric8.kubernetes.api.model.Secret
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
import org.apache.spark.security.kubernetes.constants._


private class TokenRenewService extends Actor {

  private val scheduler = newScheduler()
  private val taskBySecret = mutable.HashMap[String, RenewTask]()  // Keyed by secret UID.

  def receive: PartialFunction[Any, Unit] = {
    case AddedSecret(secret) => addTask(secret)
    case DeletedSecret(secret) => removeTask(secret)
    case AllScannedSecrets(secrets) => updateTaskSet(secrets)
    case _ =>
  }

  private def newScheduler() = Executors.newScheduledThreadPool(TOKEN_RENEW_NUM_THREADS,
    new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r, TOKEN_RENEW_THREAD_NAME)
        thread.setDaemon(true)
        thread
      }
    })

  private def addTask(secret: Secret) = {
    taskBySecret.getOrElseUpdate(uid(secret), {
      val task = new RenewTask(secret)
      scheduler.schedule(task, TOKEN_RENEW_TASK_INITIAL_DELAY_SEC, TimeUnit.SECONDS)
      task
    })
  }

  private def removeTask(secret: Secret) = {
    val task = taskBySecret.remove(uid(secret))
    task.foreach(_.cancel())
  }

  private def updateTaskSet(currentSecrets: List[Secret]) = {
    val secretByUid = currentSecrets.map(secret => (uid(secret), secret)).toMap
    val currentUids = secretByUid.keySet
    val priorUids = taskBySecret.keySet
    val uidsToAdd = currentUids -- priorUids
    uidsToAdd.foreach(uid => addTask(secretByUid(uid)))
    val uidsToRemove = priorUids -- currentUids
    uidsToRemove.foreach(uid => removeTask(secretByUid(uid)))
  }

  private def uid(secret: Secret) = secret.getMetadata.getUid
}

private class RenewTask(secret: Secret) extends Runnable {

  private val isCanceled = new AtomicBoolean

  override def run() : Unit = {

  }

  def cancel(): Unit = isCanceled.set(true)

  private def renewTokensInitially(secret: Secret) : Unit = {
    val credentials = readCredentials(secret)
    val tokens = credentials
      .flatMap(_.getAllTokens.asScala)
      .filter(_.decodeIdentifier.isInstanceOf[AbstractDelegationTokenIdentifier])
    tokens.foreach {
        token =>
          val id = token.decodeIdentifier.asInstanceOf[AbstractDelegationTokenIdentifier]
          id.getIssueDate
          id.getMaxDate
      }
  }

  private def readCredentials(secret: Secret) = {
    secret.getData.asScala
      .filterKeys(_.startsWith(HADOOP_TOKEN_KEY_IN_SECRET_DATA))
      .toSeq.sortBy(_._1)
      .lastOption
      .map {
        item =>
          val creds = new Credentials
          creds.readTokenStorageStream(
            new DataInputStream(new ByteArrayInputStream(Base64.decodeBase64(item._2))))
          creds
      }
  }
}

private case class AllScannedSecrets(secrets: List[Secret])
private case class AddedSecret(secret: Secret)
private case class DeletedSecret(secret: Secret)

private object TokenRenewService {

  def apply(system: ActorSystem) : ActorRef = system.actorOf(Props[TokenRenewService])
}

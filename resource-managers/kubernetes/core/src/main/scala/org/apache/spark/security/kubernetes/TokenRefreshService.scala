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
import java.util.concurrent.{Executors, ScheduledFuture, ThreadFactory, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import io.fabric8.kubernetes.api.model.Secret
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.spark.security.kubernetes.constants._

private class TokenRefreshService extends Actor {

  private val scheduler = newScheduler()
  // Keyed by secret UID.
  private val taskHandleBySecret = mutable.HashMap[String, ScheduledFuture[_]]()
  private val hadoopConf = new Configuration
  private val clock = new Clock

  def receive: PartialFunction[Any, Unit] = {
    case StartRefresh(secret) => addStarterTask(secret)
    case StopRefresh(secret) => removeRefreshTask(secret)
    case UpdateRefreshList(secrets) => updateRefreshTaskSet(secrets)
    case renew @ Renew(nextExpireTime, expireTimeByToken, secret) => scheduleRenewTask(renew)
    case _ =>
  }

  private def newScheduler() = Executors.newScheduledThreadPool(REFERSH_TASKS_NUM_THREADS,
    new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r, REFRESH_TASK_THREAD_NAME)
        thread.setDaemon(true)
        thread
      }
    })

  private def addStarterTask(secret: Secret) = {
    taskHandleBySecret.getOrElseUpdate(getSecretUid(secret), {
      val task = new StarterTask(secret, hadoopConf, self)
      scheduler.schedule(task, REFRESH_STARTER_TASK_INITIAL_DELAY_MILLIS, TimeUnit.MILLISECONDS)
    })
  }

  private def removeRefreshTask(secret: Secret) = {
    val task = taskHandleBySecret.remove(getSecretUid(secret))
    task.foreach(_.cancel(true))  // Interrupt if running.
  }

  private def updateRefreshTaskSet(currentSecrets: List[Secret]) = {
    val secretByUid = currentSecrets.map(secret => (getSecretUid(secret), secret)).toMap
    val currentUids = secretByUid.keySet
    val priorUids = taskHandleBySecret.keySet
    val uidsToAdd = currentUids -- priorUids
    uidsToAdd.foreach(uid => addStarterTask(secretByUid(uid)))
    val uidsToRemove = priorUids -- currentUids
    uidsToRemove.foreach(uid => removeRefreshTask(secretByUid(uid)))
  }

  private def scheduleRenewTask(renew: Renew) = {
    val uid = getSecretUid(renew.secret)
    if (taskHandleBySecret.get(uid).nonEmpty) {
      val renewTime = math.min(0L,
        renew.expireTime - RENEW_TASK_SCHEDULE_AHEAD_MILLIS - clock.nowInMillis())
      val task = new RenewTask(renew, hadoopConf, self)
      taskHandleBySecret.put(uid, scheduler.schedule(task, renewTime, TimeUnit.MILLISECONDS))
    }
  }

  private def getSecretUid(secret: Secret) = secret.getMetadata.getUid
}

private class StarterTask(secret: Secret, hadoopConf: Configuration, refreshService: ActorRef)
  extends Runnable {

  override def run() : Unit = {
    val tokens = readHadoopTokens()
    val expireTimeByToken = renewTokens(tokens)
    val nextExpireTime = expireTimeByToken.values.min
    refreshService ! Renew(nextExpireTime, expireTimeByToken, secret)
  }

  private def readHadoopTokens() = {
    val hadoopSecretData = secret.getData.asScala.filterKeys(
      _.startsWith(HADOOP_TOKEN_KEY_IN_SECRET_DATA))
    val latestData = if (hadoopSecretData.nonEmpty) Some(hadoopSecretData.max._2) else None
    val credentials = latestData.map {
      data =>
        val creds = new Credentials
        creds.readTokenStorageStream(new DataInputStream(new ByteArrayInputStream(
          Base64.decodeBase64(data))))
        creds
    }
    val tokens = credentials.map {
      creds =>
        creds.getAllTokens.asScala.toList
    }
    tokens.getOrElse(Nil)
  }

  private def renewTokens(tokens: List[Token[_ <: TokenIdentifier]])
      : Map[Token[_ <: TokenIdentifier], Long] = {
    tokens.map(token => (token, token.renew(hadoopConf))).toMap
  }
}

private class RenewTask(renew: Renew, hadoopConf: Configuration, refreshService: ActorRef)
  extends Runnable {

  override def run() : Unit = {
    val deadline = renew.expireTime + RENEW_TASK_SCHEDULE_AHEAD_MILLIS
    val newExpireTimeByToken : Map[Token[_ <: TokenIdentifier], Long] =
      renew.expireTimeByToken.map {
        item =>
          val token = item._1
          val expireTime = item._2
          val newExpireTime = if (expireTime <= deadline) {
              token.renew(hadoopConf)
            } else {
              expireTime
            }
          (token, newExpireTime)
      }
      .toMap
    val nextExpireTime = newExpireTimeByToken.values.min
    refreshService ! Renew(nextExpireTime, newExpireTimeByToken, renew.secret)
  }
}

private class Clock {

  def nowInMillis() : Long = System.currentTimeMillis()
}

private case class UpdateRefreshList(secrets: List[Secret])
private case class StartRefresh(secret: Secret)
private case class Renew(expireTime: Long,
                         expireTimeByToken: Map[Token[_ <: TokenIdentifier], Long],
                         secret: Secret)
private case class StopRefresh(secret: Secret)

private object TokenRefreshService {

  def apply(system: ActorSystem) : ActorRef = {
    UserGroupInformation.loginUserFromKeytab(
      REFRESH_SERVER_KERBEROS_PRINCIPAL,
      REFRESH_SERVER_KERBEROS_KEYTAB_PATH)
    system.actorOf(Props[TokenRefreshService])
  }
}

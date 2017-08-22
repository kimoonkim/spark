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

import org.apache.spark.internal.Logging
import org.apache.spark.security.kubernetes.constants._

private class TokenRefreshService extends Actor with Logging {

  private val scheduler = newScheduler()
  // Keyed by secret UID.
  private val taskHandleBySecret = mutable.HashMap[String, ScheduledFuture[_]]()
  private val hadoopConf = new Configuration
  private val clock = new Clock

  override def receive: PartialFunction[Any, Unit] = {
    case StartRefresh(secret) => addStarterTask(secret)
    case StopRefresh(secret) => removeRefreshTask(secret)
    case UpdateRefreshList(secrets) => updateRefreshTaskSet(secrets)
    case renew @ Renew(nextExpireTime, expireTimeByToken, secret, _) => scheduleRenewTask(renew)
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
      val task = new StarterTask(secret, hadoopConf, self, clock)
      val future = scheduler.schedule(task, REFRESH_STARTER_TASK_INITIAL_DELAY_MILLIS,
        TimeUnit.MILLISECONDS)
      logInfo(s"Started refresh of tokens in ${secret.getMetadata.getSelfLink} with ${future}")
      future
    })
  }

  private def removeRefreshTask(secret: Secret) = {
    val uid = getSecretUid(secret)
    taskHandleBySecret.remove(uid).foreach(future => {
      logInfo(s"Canceling refresh of tokens in ${secret.getMetadata.getSelfLink}")
      future.cancel(true) // Interrupt if running.
    })
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
      val numConsecutiveErrors = renew.numConsecutiveErrors
      if (numConsecutiveErrors < RENEW_TASK_MAX_CONSECUTIVE_ERRORS) {
        val renewTime = math.max(0L,
          renew.expireTime - RENEW_TASK_SCHEDULE_AHEAD_MILLIS - clock.nowInMillis())
        val task = new RenewTask(renew, hadoopConf, self, clock)
        logInfo(s"Scheduling refresh of tokens with " +
          s"${renew.secret.getMetadata.getSelfLink} at now + $renewTime millis.")
        taskHandleBySecret.put(uid, scheduler.schedule(task, renewTime, TimeUnit.MILLISECONDS))
      } else {
        logWarning(s"Got too many errors for ${renew.secret.getMetadata.getSelfLink}. Abandoning.")
        val future = taskHandleBySecret.remove(uid)
        future.foreach(_.cancel(true))  // Interrupt if running.
      }
    } else {
      logWarning(s"Could not find an entry for renew task" +
        s" ${renew.secret.getMetadata.getSelfLink}. Maybe the secret got deleted")
    }
  }

  private def getSecretUid(secret: Secret) = secret.getMetadata.getUid
}

private class StarterTask(secret: Secret,
                          hadoopConf: Configuration,
                          refreshService: ActorRef,
                          clock: Clock) extends Runnable with Logging {

  private var hasError = false

  override def run() : Unit = {
    val tokens = readHadoopTokens()
    logInfo(s"Read Hadoop tokens: $tokens")
    val expireTimeByToken = renewTokens(tokens)
    val nextExpireTime = if (expireTimeByToken.nonEmpty) {
      expireTimeByToken.values.min
    } else {
      logWarning(s"Got an empty token list with ${secret.getMetadata.getSelfLink}")
      hasError = true
      getRetryTime()
    }
    logInfo(s"Initial renew resulted with $expireTimeByToken. Next expire time $nextExpireTime")
    val numConsecutiveErrors = if (hasError) 1 else 0
    refreshService ! Renew(nextExpireTime, expireTimeByToken, secret, numConsecutiveErrors)
  }

  private def readHadoopTokens() = {
    val hadoopSecretData = secret.getData.asScala.filterKeys(
      _.startsWith(SECRET_DATA_KEY_PREFIX_HADOOP_TOKENS))
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
    tokens.map(token => {
      val expireTime = try {
          token.renew(hadoopConf)
        } catch {
          case t: Throwable =>
            logWarning(t.getMessage, t)
            hasError = true

          getRetryTime()
        }
      (token, expireTime)
    }).toMap
  }

  private def getRetryTime() = clock.nowInMillis() + RENEW_TASK_DEADLINE_LOOK_AHEAD_MILLIS
}

private class RenewTask(renew: Renew,
                        hadoopConf: Configuration,
                        refreshService: ActorRef,
                        clock: Clock) extends Runnable with Logging {

  private var hasError = false

  override def run() : Unit = {
    val deadline = renew.expireTime + RENEW_TASK_DEADLINE_LOOK_AHEAD_MILLIS
    val newExpireTimeByToken : Map[Token[_ <: TokenIdentifier], Long] =
      renew.expireTimeByToken.map {
        item =>
          val token = item._1
          val expireTime = item._2
          val newExpireTime =
            if (expireTime <= deadline) {
              try {
                token.renew(hadoopConf)
              } catch {
                case t: Throwable =>
                  logWarning(t.getMessage, t)
                  hasError = true

                getRetryTime()
              }
            } else {
              expireTime
            }
          (token, newExpireTime)
      }
      .toMap
    if (newExpireTimeByToken.nonEmpty) {
      val nextExpireTime = newExpireTimeByToken.values.min
      logInfo(s"Renewed with the result $newExpireTimeByToken. Next expire time $nextExpireTime")
      val numConsecutiveErrors = if (hasError) renew.numConsecutiveErrors + 1 else 0
      refreshService ! Renew(nextExpireTime, newExpireTimeByToken, renew.secret,
        numConsecutiveErrors)
    } else {
      logWarning(s"Got an empty token list with ${renew.secret.getMetadata.getSelfLink}")
    }
  }

  private def getRetryTime() = clock.nowInMillis() + RENEW_TASK_DEADLINE_LOOK_AHEAD_MILLIS
}

private class Clock {

  def nowInMillis() : Long = System.currentTimeMillis()
}

private case class UpdateRefreshList(secrets: List[Secret])
private case class StartRefresh(secret: Secret)
private case class Renew(expireTime: Long,
                         expireTimeByToken: Map[Token[_ <: TokenIdentifier], Long],
                         secret: Secret,
                         numConsecutiveErrors: Int)
private case class StopRefresh(secret: Secret)

private object TokenRefreshService {

  def apply(system: ActorSystem) : ActorRef = {
    UserGroupInformation.loginUserFromKeytab(
      REFRESH_SERVER_KERBEROS_PRINCIPAL,
      REFRESH_SERVER_KERBEROS_KEYTAB_PATH)
    system.actorOf(Props[TokenRefreshService])
  }
}

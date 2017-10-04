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
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props}
import io.fabric8.kubernetes.api.model.Secret
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.spark.security.kubernetes.constants._


private class TokenRefreshService extends Actor with Logging {

  private val scheduler = context.system.scheduler
  // Keyed by secret UID.
  private val taskHandleBySecret = mutable.HashMap[String, Cancellable]()
  private val hadoopConf = new Configuration
  private val clock = new Clock

  override def receive: PartialFunction[Any, Unit] = {
    case Relogin => launchReloginTask()
    case StartRefresh(secret) => addStarterTask(secret)
    case StopRefresh(secret) => removeRefreshTask(secret)
    case UpdateRefreshList(secrets) => updateRefreshTaskSet(secrets)
    case renew @ Renew(nextExpireTime, expireTimeByToken, secret, _) => scheduleRenewTask(renew)
    case _ =>
  }

  private def launchReloginTask() = {
    val task = new ReloginTask
    scheduler.scheduleOnce(Duration(0L, TimeUnit.MILLISECONDS), task)
  }

  private def addStarterTask(secret: Secret) = {
    taskHandleBySecret.getOrElseUpdate(getSecretUid(secret), {
      val task = new StarterTask(secret, hadoopConf, self, clock)
      val cancellable = scheduler.scheduleOnce(
        Duration(REFRESH_STARTER_TASK_INITIAL_DELAY_MILLIS, TimeUnit.MILLISECONDS),
        task)
      logInfo(s"Started refresh of tokens in ${secret.getMetadata.getSelfLink} with ${cancellable}")
      cancellable
    })
  }

  private def removeRefreshTask(secret: Secret) : Unit = {
    val uid = getSecretUid(secret)
    taskHandleBySecret.remove(uid).foreach(cancellable => {
      logInfo(s"Canceling refresh of tokens in ${secret.getMetadata.getSelfLink}")
      cancellable.cancel()
    })
  }

  private def updateRefreshTaskSet(currentSecrets: List[Secret]) : Unit = {
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
        val durationTillExpire = math.max(0L, renew.expireTime - clock.nowInMillis())
        val renewTime = math.max(0L, renew.expireTime - durationTillExpire / 10)  // 90% mark.
        val durationTillRenew = math.max(0L, renewTime - clock.nowInMillis())
        val task = new RenewTask(renew, hadoopConf, self, clock)
        logInfo(s"Scheduling refresh of tokens with " +
          s"${renew.secret.getMetadata.getSelfLink} at now + $durationTillRenew millis.")
        val cancellable = scheduler.scheduleOnce(
          Duration(durationTillRenew, TimeUnit.MILLISECONDS), task)
        taskHandleBySecret.put(uid, cancellable)
      } else {
        logWarning(s"Got too many errors for ${renew.secret.getMetadata.getSelfLink}. Abandoning.")
        val maybeCancellable = taskHandleBySecret.remove(uid)
        maybeCancellable.foreach(_.cancel())
      }
    } else {
      logWarning(s"Could not find an entry for renew task" +
        s" ${renew.secret.getMetadata.getSelfLink}. Maybe the secret got deleted")
    }
  }

  private def getSecretUid(secret: Secret) = secret.getMetadata.getUid
}

private class ReloginTask extends Runnable {

  override def run() : Unit = {
    UserGroupInformation.getLoginUser.checkTGTAndReloginFromKeytab()
  }
}

private class StarterTask(secret: Secret,
                          hadoopConf: Configuration,
                          refreshService: ActorRef,
                          clock: Clock) extends Runnable with Logging {

  private var hasError = false

  override def run() : Unit = {
    val expireTimeByToken = readHadoopTokens()
    logInfo(s"Read Hadoop tokens: $expireTimeByToken")
    val nextExpireTime = if (expireTimeByToken.nonEmpty) {
      expireTimeByToken.values.min
    } else {
      logWarning(s"Got an empty token list with ${secret.getMetadata.getSelfLink}")
      hasError = true
      getRetryTime
    }
    logInfo(s"Initial renew resulted with $expireTimeByToken. Next expire time $nextExpireTime")
    val numConsecutiveErrors = if (hasError) 1 else 0
    refreshService ! Renew(nextExpireTime, expireTimeByToken, secret, numConsecutiveErrors)
  }

  private def readHadoopTokens() : Map[Token[_ <: TokenIdentifier], Long] = {
    val hadoopSecretData = secret.getData.asScala.filterKeys(
      _.startsWith(SECRET_DATA_KEY_PREFIX_HADOOP_TOKENS))
    val latestData = if (hadoopSecretData.nonEmpty) Some(hadoopSecretData.max) else None
    latestData.map {
      item =>
        val key = item._1
        val data = item._2
        val createTimeAndDuration = key.split(SECRET_DATA_KEY_REGEX_HADOOP_TOKENS, 2)
        val expireTime = createTimeAndDuration(0).toLong + createTimeAndDuration(1).toLong
        val creds = new Credentials
        creds.readTokenStorageStream(new DataInputStream(new ByteArrayInputStream(
          Base64.decodeBase64(data))))
        creds.getAllTokens.asScala.toList.map {
          (_, expireTime)
        }
    }.toList.flatten.toMap
  }

  private def getRetryTime = clock.nowInMillis() + RENEW_TASK_RETRY_TIME_MILLIS
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

                getRetryTime
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

  private def getRetryTime = clock.nowInMillis() + RENEW_TASK_RETRY_TIME_MILLIS
}

private class Clock {

  def nowInMillis() : Long = System.currentTimeMillis()
}

private sealed trait Command
private case object Relogin extends Command
private case class UpdateRefreshList(secrets: List[Secret]) extends Command
private case class StartRefresh(secret: Secret) extends Command
private case class Renew(expireTime: Long,
                         expireTimeByToken: Map[Token[_ <: TokenIdentifier], Long],
                         secret: Secret,
                         numConsecutiveErrors: Int) extends Command
private case class StopRefresh(secret: Secret) extends Command

private object TokenRefreshService {

  def apply(system: ActorSystem) : ActorRef = {
    UserGroupInformation.loginUserFromKeytab(
      REFRESH_SERVER_KERBEROS_PRINCIPAL,
      REFRESH_SERVER_KERBEROS_KEYTAB_PATH)
    val actor = system.actorOf(Props[TokenRefreshService])
    val duration = Duration(REFRESH_SERVER_KERBEROS_RELOGIN_PERIOD_MILLIS, TimeUnit.MILLISECONDS)
    system.scheduler.schedule(duration, duration, actor, Relogin)
    actor
  }
}

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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.security.PrivilegedExceptionAction
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props}
import io.fabric8.kubernetes.api.model.{ObjectMeta, Secret}
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.spark.security.kubernetes.constants._


private class TokenRefreshService(kubernetesClient: KubernetesClient) extends Actor with Logging {

  private val scheduler = context.system.scheduler
  private val secretUidToTaskHandle = mutable.HashMap[String, Cancellable]()
  private val recentlyAddedSecretUids = mutable.HashSet[String]()
  private val hadoopConf = new Configuration
  private val clock = new Clock

  override def receive: PartialFunction[Any, Unit] = {
    case Relogin =>
      launchReloginTask()
    case StartRefresh(secret) =>
      startRefreshTask(secret)
    case StopRefresh(secret) =>
      removeRefreshTask(secret)
    case UpdateSecretsToTrack(secrets) =>
      updateSecretsToTrack(secrets)
    case renew: Renew =>
      scheduleRenewTask(renew)
  }

  override def postStop(): Unit = {
    super.postStop()
    secretUidToTaskHandle.values.map(_.cancel)
  }

  private def launchReloginTask() = {
    val task = new ReloginTask
    scheduler.scheduleOnce(Duration(0L, TimeUnit.MILLISECONDS), task)
  }

  private def startRefreshTask(secret: Secret) = {
    recentlyAddedSecretUids.add(getSecretUid(secret.getMetadata))
    addRefreshTask(secret)
  }

  private def addRefreshTask(secret: Secret) = {
    secretUidToTaskHandle.getOrElseUpdate(getSecretUid(secret.getMetadata), {
      val task = new StarterTask(secret, hadoopConf, self, clock)
      val cancellable = scheduler.scheduleOnce(
        Duration(STARTER_TASK_INITIAL_DELAY_MILLIS, TimeUnit.MILLISECONDS),
        task)
      logInfo(s"Started refresh of tokens in ${secret.getMetadata.getSelfLink} with $cancellable")
      cancellable
    })
  }

  private def removeRefreshTask(secret: Secret) : Unit = {
    val uid = getSecretUid(secret.getMetadata)
    secretUidToTaskHandle.remove(uid).foreach(cancellable => {
      logInfo(s"Canceling refresh of tokens in ${secret.getMetadata.getSelfLink}")
      cancellable.cancel()
    })
  }

  private def updateSecretsToTrack(currentSecrets: List[Secret]) : Unit = {
    val secretsByUids = currentSecrets.map(secret => (getSecretUid(secret.getMetadata), secret))
      .toMap
    val currentUids = secretsByUids.keySet
    val priorUids = secretUidToTaskHandle.keySet
    val uidsToAdd = currentUids -- priorUids
    uidsToAdd.foreach(uid => addRefreshTask(secretsByUids(uid)))
    val uidsToRemove = priorUids -- currentUids -- recentlyAddedSecretUids
    uidsToRemove.foreach(uid => removeRefreshTask(secretsByUids(uid)))
    recentlyAddedSecretUids.clear()
  }

  private def scheduleRenewTask(renew: Renew) = {
    val uid = getSecretUid(renew.secretMeta)
    if (secretUidToTaskHandle.get(uid).nonEmpty) {
      val numConsecutiveErrors = renew.numConsecutiveErrors
      if (numConsecutiveErrors < RENEW_TASK_MAX_CONSECUTIVE_ERRORS) {
        val durationTillExpire = math.max(0L, renew.expireTime - clock.nowInMillis())
        val renewTime = math.max(0L, renew.expireTime - durationTillExpire / 10)  // 90% mark.
        val durationTillRenew = math.max(0L, renewTime - clock.nowInMillis())
        val task = new RenewTask(renew, hadoopConf, self, kubernetesClient, clock)
        logInfo(s"Scheduling refresh of tokens with " +
          s"${renew.secretMeta.getSelfLink} at now + $durationTillRenew millis.")
        val cancellable = scheduler.scheduleOnce(
          Duration(durationTillRenew, TimeUnit.MILLISECONDS), task)
        secretUidToTaskHandle.put(uid, cancellable)
      } else {
        logWarning(s"Got too many errors for ${renew.secretMeta.getSelfLink}. Abandoning.")
        val maybeCancellable = secretUidToTaskHandle.remove(uid)
        maybeCancellable.foreach(_.cancel())
      }
    } else {
      logWarning(s"Could not find an entry for renew task" +
        s" ${renew.secretMeta.getSelfLink}. Maybe the secret got deleted")
    }
  }

  private def getSecretUid(secret: ObjectMeta) = secret.getUid
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
    val tokenToExpireTime = readTokensFromSecret()
    logInfo(s"Read Hadoop tokens: $tokenToExpireTime")
    val nextExpireTime = if (tokenToExpireTime.nonEmpty) {
      tokenToExpireTime.values.min
    } else {
      logWarning(s"Got an empty token list with ${secret.getMetadata.getSelfLink}")
      hasError = true
      getRetryTime
    }
    val numConsecutiveErrors = if (hasError) 1 else 0
    refreshService ! Renew(nextExpireTime, tokenToExpireTime, secret.getMetadata,
      numConsecutiveErrors)
  }

  private def readTokensFromSecret() : Map[Token[_ <: TokenIdentifier], Long] = {
    val dataItems = secret.getData.asScala.filterKeys(
      _.startsWith(SECRET_DATA_ITEM_KEY_PREFIX_HADOOP_TOKENS)).toSeq.sorted
    val latestDataItem = if (dataItems.nonEmpty) Some(dataItems.max) else None
    latestDataItem.map {
      case (key, data) =>
        val matcher = TokenRefreshService.hadoopTokenPattern.matcher(key)
        val matches = matcher.matches()
        logInfo(s"Matching secret data $key, result $matches")
        val createTime = matcher.group(1).toLong
        val duration = matcher.group(2).toLong
        val expireTime = createTime + duration
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
                        kubernetesClient: KubernetesClient,
                        clock: Clock) extends Runnable with Logging {

  private var hasError = false

  override def run() : Unit = {
    val deadline = renew.expireTime + RENEW_TASK_DEADLINE_LOOK_AHEAD_MILLIS
    val nowMillis = clock.nowInMillis()
    val newExpireTimeByToken : Map[Token[_ <: TokenIdentifier], Long] =
      renew.tokenToExpireTime.map {
        case (token, expireTime) =>
          val (maybeNewToken, maybeNewExpireTime) = refresh(token, expireTime, deadline, nowMillis)
          (maybeNewToken, maybeNewExpireTime)
      }
      .toMap
    if (newExpireTimeByToken.nonEmpty) {
      val newTokens = newExpireTimeByToken.keySet -- renew.tokenToExpireTime.keySet
      if (newTokens.nonEmpty) {
        writeTokensToSecret(newExpireTimeByToken, nowMillis)
      }
      val nextExpireTime = newExpireTimeByToken.values.min
      logInfo(s"Renewed tokens $newExpireTimeByToken. Next expire time $nextExpireTime")
      val numConsecutiveErrors = if (hasError) renew.numConsecutiveErrors + 1 else 0
      refreshService ! Renew(nextExpireTime, newExpireTimeByToken, renew.secretMeta,
        numConsecutiveErrors)
    } else {
      logWarning(s"Got an empty token list with ${renew.secretMeta.getSelfLink}")
    }
  }

  private def refresh(token: Token[_ <: TokenIdentifier], expireTime: Long, deadline: Long,
                      nowMillis: Long) = {
    val maybeNewToken = maybeObtainNewToken(token, expireTime, nowMillis)
    val maybeNewExpireTime = maybeGetNewExpireTime(maybeNewToken, expireTime, deadline, nowMillis)
    (maybeNewToken, maybeNewExpireTime)
  }

  private def maybeObtainNewToken(token: Token[_ <: TokenIdentifier], expireTime: Long,
                                  nowMills: Long) = {
    val maybeNewToken = if (token.getKind.equals(DelegationTokenIdentifier.HDFS_DELEGATION_KIND)) {
      // The token can casted to AbstractDelegationTokenIdentifier below only if the token kind
      // is HDFS_DELEGATION_KIND, according to the YARN resource manager code. See if this can be
      // generalized beyond HDFS tokens.
      val identifier = token.decodeIdentifier().asInstanceOf[AbstractDelegationTokenIdentifier]
      val maxDate = identifier.getMaxDate
      if (maxDate - expireTime < RENEW_TASK_REMAINING_TIME_BEFORE_NEW_TOKEN_MILLIS ||
        maxDate <= nowMills) {
        logDebug(s"Obtaining a new token with maxData $maxDate," +
          s" expireTime $expireTime, now $nowMills")
        val newToken = obtainNewToken(token, identifier)
        logInfo(s"Obtained token $newToken")
        newToken
      } else {
        token
      }
    } else {
      token
    }
    maybeNewToken
  }

  private def maybeGetNewExpireTime(token: Token[_ <: TokenIdentifier], expireTime: Long,
                                    deadline: Long,
                                    nowMillis: Long) = {
    if (expireTime <= deadline || expireTime <= nowMillis) {
      try {
        logDebug(s"Renewing token $token with current expire time $expireTime," +
          s" deadline $deadline, now $nowMillis")
        val newExpireTime = token.renew(hadoopConf)
        logDebug(s"Renewed token $token. Next expire time $newExpireTime")
        newExpireTime
      } catch {
        case t: Throwable =>
          logWarning(t.getMessage, t)
          hasError = true

          getRetryTime
      }
    } else {
      expireTime
    }
  }

  private def obtainNewToken(token: Token[_ <: TokenIdentifier],
                             identifier: AbstractDelegationTokenIdentifier) = {
    val owner = identifier.getOwner
    val realUser = identifier.getRealUser
    val user = if (realUser == null || realUser.toString.isEmpty || realUser.equals(owner)) {
      owner.toString
    } else {
      realUser.toString
    }
    val credentials = new Credentials
    val ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser)
    val newToken = ugi.doAs(new PrivilegedExceptionAction[Token[_ <: TokenIdentifier]] {

      override def run() : Token[_ <: TokenIdentifier] = {
        val fs = FileSystem.get(hadoopConf)
        val tokens = fs.addDelegationTokens(UserGroupInformation.getLoginUser.getUserName,
          credentials)
        tokens(0)
      }
    })
    newToken
  }

  private def writeTokensToSecret(tokenToExpire: Map[Token[_ <: TokenIdentifier], Long],
                                  nowMillis: Long) : Unit = {
    val durationUntilExpire = tokenToExpire.values.min - nowMillis
    val key = s"$SECRET_DATA_ITEM_KEY_PREFIX_HADOOP_TOKENS$nowMillis-$durationUntilExpire"
    val credentials = new Credentials()
    tokenToExpire.keys.foreach(token => credentials.addToken(token.getService, token))
    val serialized = serializeCredentials(credentials)
    val value = Base64.encodeBase64String(serialized)
    val secretMeta = renew.secretMeta
    val editor = kubernetesClient.secrets
      .inNamespace(secretMeta.getNamespace)
      .withName(secretMeta.getName)
      .edit()
    editor.addToData(key, value)
    val dataItemKeys = editor.getData.keySet().asScala.filter(
      _.startsWith(SECRET_DATA_ITEM_KEY_PREFIX_HADOOP_TOKENS)).toSeq.sorted
    // Remove data items except the latest two data items. A K8s secret can hold only up to 1 MB
    // data. We need to remove old data items. We keep the latest two items to avoid race conditions
    // where some newly launching executors may access the previous token.
    dataItemKeys.dropRight(2).foreach(editor.removeFromData)
    editor.done
    logInfo(s"Wrote new tokens $tokenToExpire to a data item $key in ${secretMeta.getSelfLink}")
  }

  private def serializeCredentials(credentials: Credentials) = {
    val byteStream = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(byteStream)
    credentials.writeTokenStorageToStream(dataStream)
    dataStream.flush()
    byteStream.toByteArray
  }

  private def getRetryTime = clock.nowInMillis() + RENEW_TASK_RETRY_TIME_MILLIS
}

private class Clock {

  def nowInMillis() : Long = System.currentTimeMillis()
}

private sealed trait Command
private case object Relogin extends Command
private case class UpdateSecretsToTrack(secrets: List[Secret]) extends Command
private case class StartRefresh(secret: Secret) extends Command
private case class Renew(expireTime: Long,
                         tokenToExpireTime: Map[Token[_ <: TokenIdentifier], Long],
                         secretMeta: ObjectMeta,
                         numConsecutiveErrors: Int) extends Command
private case class StopRefresh(secret: Secret) extends Command

private object TokenRefreshService {

  val hadoopTokenPattern = Pattern.compile(SECRET_DATA_ITEM_KEY_REGEX_HADOOP_TOKENS)

  def apply(system: ActorSystem, kubernetesClient: KubernetesClient) : ActorRef = {
    UserGroupInformation.loginUserFromKeytab(
      REFRESH_SERVER_KERBEROS_PRINCIPAL,
      REFRESH_SERVER_KERBEROS_KEYTAB_PATH)
    val actor = system.actorOf(Props(classOf[TokenRefreshService], kubernetesClient))
    val duration = Duration(REFRESH_SERVER_KERBEROS_RELOGIN_PERIOD_MILLIS, TimeUnit.MILLISECONDS)
    system.scheduler.schedule(duration, duration, actor, Relogin)
    actor
  }
}

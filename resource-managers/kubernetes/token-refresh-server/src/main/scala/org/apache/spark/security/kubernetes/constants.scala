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

package object constants {

  val REFRESH_SERVER_KERBEROS_PRINCIPAL = "kimoonkim"
  val REFRESH_SERVER_KERBEROS_KEYTAB_PATH = "/mnt/secrets/krb5.keytab"
  val REFRESH_SERVER_KERBEROS_RELOGIN_PERIOD_MILLIS = 60 * 60 * 1000L

  val SECRET_LABEL_KEY_REFRESH_HADOOP_TOKENS = "refresh-hadoop-tokens"
  val SECRET_LABEL_VALUE_REFRESH_HADOOP_TOKENS = "yes"
  val SECRET_DATA_KEY_PREFIX_HADOOP_TOKENS = "hadoop-tokens-"

  val REFERSH_TASKS_NUM_THREADS = 10
  val REFRESH_TASK_THREAD_NAME = "token-renewer"
  val REFRESH_STARTER_TASK_INITIAL_DELAY_MILLIS = 0L
  val RENEW_TASK_RETRY_TIME_MILLIS = 10000L
  val RENEW_TASK_DEADLINE_LOOK_AHEAD_MILLIS = 10000L
  val RENEW_TASK_SCHEDULE_AHEAD_MILLIS = 10000L
  val RENEW_TASK_MAX_CONSECUTIVE_ERRORS = 3

  val SECRET_SCANNER_THREAD_NAME = "secret-scanner"
  val SECRET_SCANNER_INITIAL_DELAY_MILLIS = 10 * 1000L
  val SECRET_SCANNER_PERIOD_MILLIS = 60 * 60 * 1000L
  val IS_DAEMON_THREAD = true
}

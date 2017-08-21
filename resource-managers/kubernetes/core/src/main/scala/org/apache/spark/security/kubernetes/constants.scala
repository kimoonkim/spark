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

  val HADOOP_DELEGATION_TOKEN_LABEL_IN_SECRET = "hadoop.delegation.token"
  val HADOOP_TOKEN_KEY_IN_SECRET_DATA = "hadoop-token"

  val TOKEN_RENEW_NUM_THREADS = 10
  val TOKEN_RENEW_THREAD_NAME = "token-renewer"
  val TOKEN_RENEW_TASK_INITIAL_DELAY_MILLIS = 0L
  val TOKEN_RENEW_SCHEDULE_AHEAD_MILLIS = 10000L

  val SECRET_FIND_THREAD_NAME = "secret-finder"
  val IS_DAEMON_THREAD = true
}

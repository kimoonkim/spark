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
package org.apache.spark.scheduler.cluster.kubernetes

import org.apache.spark.SparkContext
import org.apache.spark.scheduler._

import scala.collection.mutable.ArrayBuffer

private[spark] class KubernetesClusterManager extends ExternalClusterManager {

  var clusterSchedulerBackend : Option[KubernetesClusterSchedulerBackend]


  override def canCreate(masterURL: String): Boolean = masterURL.startsWith("k8s")

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    val scheduler = new KubernetesTaskSchedulerImpl(sc)
    sc.taskScheduler = scheduler
    scheduler
  }

  override def createSchedulerBackend(sc: SparkContext, masterURL: String, scheduler: TaskScheduler)
      : SchedulerBackend = {
    val schedulerBackend = new KubernetesClusterSchedulerBackend(
      sc.taskScheduler.asInstanceOf[TaskSchedulerImpl], sc)
    this.clusterSchedulerBackend = Some(schedulerBackend)
    schedulerBackend
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }

  private[spark] class KubernetesTaskSchedulerImpl(sc: SparkContext) extends TaskSchedulerImpl(sc) {

    override def createTaskSetManager(taskSet: TaskSet, maxTaskFailures: Int): TaskSetManager = {
      new KubernetesTaskSetManager(this, taskSet, maxTaskFailures)
    }
  }

  private[spark] class KubernetesTaskSetManager(
       taskScheduler: KubernetesTaskSchedulerImpl, taskSet: TaskSet, maxTaskFailures: Int)
    extends TaskSetManager(taskScheduler, taskSet, maxTaskFailures) {

    override def getPendingTasksForHost(host: String): ArrayBuffer[Int] = {
      var key = clusterSchedulerBackend.get.getClusterNodeForExecutor(host).getOrElse(host)
      super.getPendingTasksForHost(key)
    }
  }
}


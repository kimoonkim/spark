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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkContext
import org.apache.spark.scheduler._

import scala.collection.mutable.ArrayBuffer

private[spark] class KubernetesClusterManager extends ExternalClusterManager {

  var clusterSchedulerBackend : AtomicReference[KubernetesClusterSchedulerBackend] =
    new AtomicReference()

  override def canCreate(masterURL: String): Boolean = masterURL.startsWith("k8s")

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    val scheduler = new TaskSchedulerImpl(sc) {

      override def createTaskSetManager(taskSet: TaskSet, maxTaskFailures: Int): TaskSetManager = {
        new TaskSetManager(sched = this, taskSet, maxTaskFailures) {

          // Returns preferred tasks for an executor that may have local data there,
          // using the physical cluster node name that it is running on.
          override def getPendingTasksForHost(host: String): ArrayBuffer[Int] = {
            var pendingTasks = super.getPendingTasksForHost(host)
            if (pendingTasks.nonEmpty) {
              return pendingTasks
            }
            val emptyTasks = pendingTasks
            val backend = clusterSchedulerBackend.get
            if (backend == null) {
              return emptyTasks
            }
            val clusterNode = backend.getClusterNodeForExecutor(host)
            if (clusterNode.isEmpty) {
              return emptyTasks
            }
            pendingTasks = super.getPendingTasksForHost(clusterNode.get)
            if (pendingTasks.nonEmpty) {
              logInfo(s"Got preferred task list $pendingTasks for executor host $host " +
                s"using cluster node name $clusterNode")
            }
            pendingTasks
          }
        }
      }
    }
    sc.taskScheduler = scheduler
    scheduler
  }

  override def createSchedulerBackend(sc: SparkContext, masterURL: String, scheduler: TaskScheduler)
      : SchedulerBackend = {
    val schedulerBackend = new KubernetesClusterSchedulerBackend(
      sc.taskScheduler.asInstanceOf[TaskSchedulerImpl], sc)
    this.clusterSchedulerBackend.set(schedulerBackend)
    schedulerBackend
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}


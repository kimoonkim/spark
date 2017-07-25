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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.net.{NetworkTopology, ScriptBasedMapping, TableMapping}
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}

import org.apache.spark.scheduler.{SchedulerBackend, TaskSchedulerImpl, TaskSet, TaskSetManager}
import org.apache.spark.util.Utils
import org.apache.spark.SparkContext

private[spark] class KubernetesTaskSchedulerImpl(
    sc: SparkContext,
    rackResolverUtil: RackResolverUtil,
    inetAddressUtil: InetAddressUtil = new InetAddressUtil) extends TaskSchedulerImpl(sc) {

  var kubernetesSchedulerBackend: KubernetesClusterSchedulerBackend = null

  def this(sc: SparkContext) = this(sc, new RackResolverUtil(sc.hadoopConfiguration))

  override def initialize(backend: SchedulerBackend): Unit = {
    super.initialize(backend)
    kubernetesSchedulerBackend = this.backend.asInstanceOf[KubernetesClusterSchedulerBackend]
  }
  override def createTaskSetManager(taskSet: TaskSet, maxTaskFailures: Int): TaskSetManager = {
    new KubernetesTaskSetManager(this, taskSet, maxTaskFailures)
  }

  override def getRackForHost(hostPort: String): Option[String] = {
    if (!rackResolverUtil.isConfigured) {
      // Only calls resolver when it is configured to avoid sending DNS queries for cluster nodes.
      // See InetAddressUtil for details.
      None
    } else {
      getRackForDatanodeOrExecutor(hostPort)
    }
  }

  private def getRackForDatanodeOrExecutor(hostPort: String): Option[String] = {
    val host = Utils.parseHostPort(hostPort)._1
    val executorPod = kubernetesSchedulerBackend.getExecutorPodByIP(host)
    val hadoopConfiguration = sc.hadoopConfiguration
    executorPod.map(
        pod => {
          val clusterNodeName = pod.getSpec.getNodeName
          val rackByNodeName = rackResolverUtil.resolveRack(hadoopConfiguration, clusterNodeName)
          rackByNodeName.orElse({
            val clusterNodeIP = pod.getStatus.getHostIP
            val rackByNodeIP = rackResolverUtil.resolveRack(hadoopConfiguration, clusterNodeIP)
            rackByNodeIP.orElse({
              val clusterNodeFullName = inetAddressUtil.getFullHostName(clusterNodeIP)
              rackResolverUtil.resolveRack(hadoopConfiguration, clusterNodeFullName)
            })
          })
        }
      ).getOrElse(rackResolverUtil.resolveRack(hadoopConfiguration, host))
  }
}

private[kubernetes] class RackResolverUtil(hadoopConfiguration: Configuration) {

  val scriptPlugin : String = classOf[ScriptBasedMapping].getCanonicalName
  val tablePlugin : String = classOf[TableMapping].getCanonicalName
  val isConfigured : Boolean = checkConfigured(hadoopConfiguration)

  // RackResolver logs an INFO message whenever it resolves a rack, which is way too often.
  if (Logger.getLogger(classOf[RackResolver]).getLevel == null) {
    Logger.getLogger(classOf[RackResolver]).setLevel(Level.WARN)
  }

  def checkConfigured(hadoopConfiguration: Configuration): Boolean = {
    val plugin = hadoopConfiguration.get(
      CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY, scriptPlugin)
    val scriptName = hadoopConfiguration.get(
      CommonConfigurationKeysPublic.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY, "")
    val tableName = hadoopConfiguration.get(
      CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, "")
    plugin == scriptPlugin && scriptName.nonEmpty ||
      plugin == tablePlugin && tableName.nonEmpty ||
      plugin != scriptPlugin && plugin != tablePlugin
  }

  def resolveRack(hadoopConfiguration: Configuration, host: String): Option[String] = {
    val rack = Option(RackResolver.resolve(hadoopConfiguration, host).getNetworkLocation)
    if (rack.nonEmpty && rack.get != NetworkTopology.DEFAULT_RACK) {
      rack
    } else {
      None
    }
  }
}

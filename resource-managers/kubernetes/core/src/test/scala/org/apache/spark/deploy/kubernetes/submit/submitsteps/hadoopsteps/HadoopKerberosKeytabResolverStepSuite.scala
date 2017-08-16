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
package org.apache.spark.deploy.kubernetes.submit.submitsteps.hadoopsteps

import java.io.File
import java.util.UUID

import scala.collection.JavaConverters._

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.util.Utils



private[spark] class HadoopKerberosKeytabResolverStepSuite extends SparkFunSuite {
  private val POD_LABEL = Map("bootstrap" -> "true")
  private val DRIVER_CONTAINER_NAME = "driver-container"
  private val TEMP_KEYTAB_FILE = createTempFile("keytab")
  private val KERB_PRINCIPAL = "user@k8s.com"

  // TODO: Require mocking of UGI methods
  test("Testing keytab login") {
    val keytabStep = new HadoopKerberosKeytabResolverStep(
      new SparkConf(),
      Some(KERB_PRINCIPAL),
      Some(TEMP_KEYTAB_FILE))
    val hadoopConfSpec = HadoopConfigSpec(
      Map.empty[String, String],
      new PodBuilder()
        .withNewMetadata()
        .addToLabels("bootstrap", "true")
        .endMetadata()
        .withNewSpec().endSpec()
        .build(),
      new ContainerBuilder().withName(DRIVER_CONTAINER_NAME).build(),
      Map.empty[String, String],
      None,
      "",
      "")
    val returnContainerSpec = keytabStep.configureContainers(hadoopConfSpec)
    assert(returnContainerSpec.additionalDriverSparkConf(HADOOP_KERBEROS_CONF_LABEL)
        .contains(KERBEROS_SECRET_LABEL_PREFIX))
    assert(returnContainerSpec.additionalDriverSparkConf(HADOOP_KERBEROS_CONF_SECRET) ===
      HADOOP_KERBEROS_SECRET_NAME)
    assert(returnContainerSpec.driverContainer.getName == DRIVER_CONTAINER_NAME)
    assert(returnContainerSpec.driverPod.getMetadata.getLabels.asScala === POD_LABEL)
    assert(returnContainerSpec.dtSecretLabel.contains(KERBEROS_SECRET_LABEL_PREFIX))
    assert(returnContainerSpec.dtSecretName === HADOOP_KERBEROS_SECRET_NAME)
    assert(returnContainerSpec.dtSecret.nonEmpty)
    assert(returnContainerSpec.dtSecret.get.getMetadata.getName === HADOOP_KERBEROS_SECRET_NAME)
  }

  private def createTempFile(contents: String): File = {
    val dir = Utils.createTempDir()
    val file = new File(dir, s"${UUID.randomUUID().toString}")
    Files.write(contents.getBytes, file)
    file
  }
}

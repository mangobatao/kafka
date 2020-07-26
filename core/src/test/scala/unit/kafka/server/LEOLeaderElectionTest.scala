/**
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

package kafka.server

import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import kafka.utils.{CoreUtils, TestUtils, ZkUtils}
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.controller.{ControllerChannelManager, ControllerContext, KafkaController, LeaderUtils, OfflinePartitionLeaderSelector}
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.junit.{After, Before, Test}

class LEOLeaderElectionTest extends ZooKeeperTestHarness {
  val brokerId0 = 0
  val brokerId1 = 1
  val brokerId2 = 2
  val brokerIds = List(brokerId0, brokerId1, brokerId2)

  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

  @Before
  override def setUp() {
    super.setUp()

    val configProps0 = TestUtils.createBrokerConfig(brokerId0, zkConnect, enableControlledShutdown = false)
    val configProps1 = TestUtils.createBrokerConfig(brokerId1, zkConnect, enableControlledShutdown = false)
    val configProps2 = TestUtils.createBrokerConfig(brokerId2, zkConnect, enableControlledShutdown = false)
    // enable the quorum based ack
    configProps0.setProperty(KafkaConfig.QuorumAckReplicasProp, "2")
    configProps1.setProperty(KafkaConfig.QuorumAckReplicasProp, "2")
    configProps2.setProperty(KafkaConfig.QuorumAckReplicasProp, "2")

    // start all servers
    val server0 = TestUtils.createServer(KafkaConfig.fromProps(configProps0))
    val server1 = TestUtils.createServer(KafkaConfig.fromProps(configProps1))
    val server2 = TestUtils.createServer(KafkaConfig.fromProps(configProps2))
    servers ++= List(server0, server1, server2)
  }

  @After
  override def tearDown() {
    servers.foreach(_.shutdown())
    servers.foreach(server => CoreUtils.delete(server.config.logDirs))
    super.tearDown()
  }

  @Test
  def testLEOLeaderElection {
    // start 3 brokers
    val topic = "new-topic"
    val partitionId = 0

    // create topic with 1 partition, 3 replicas, one on each broker
    val leader1 = createTopic(zkUtils, topic, partitionReplicaAssignment = Map(0 -> Seq(0, 1, 2)), servers = servers)(0)

    val leaderEpoch1 = zkUtils.getEpochForPartition(topic, partitionId)
    debug("leader Epoc: " + leaderEpoch1)
    debug("Leader is elected to be: %s".format(leader1.getOrElse(-1)))
    assertTrue("Leader should get elected", leader1.isDefined)
    // NOTE: this is to avoid transient test failures
    assertTrue("Leader could be broker 0, broker 1 or broker 2", (leader1.getOrElse(-1) == 0) || (leader1.getOrElse(-1) == 1) || (leader1.getOrElse(-1) == 2))
    assertEquals("First epoch value should be 0", 0, leaderEpoch1)

    // fresh new replicas should start with LEO equals to 0
    val controllerChannelManager = createControllerChannelManager
    controllerChannelManager.startup()
    val responses = brokerIds.map(broker => controllerChannelManager.blockingSendRequest(broker, new TopicPartition(topic, partitionId)))
    responses.foreach(response => assertEquals(response.logEndOffset, 0L))

    // shutdown broker 2 ReplicaFetcherManager to make it lag behind
    servers(2).replicaManager.replicaFetcherManager.shutdown

    val producer = createNewProducer(TestUtils.getBrokerListStrFromServers(servers))
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, partitionId, "key".getBytes, "value".getBytes)
    producer.send(record).get()

    // broker 2 will lag behind from the LEO's perspective
    assertEquals(controllerChannelManager.blockingSendRequest(brokerId1, new TopicPartition(topic, partitionId)).logEndOffset, 1L)
    assertEquals(controllerChannelManager.blockingSendRequest(brokerId2, new TopicPartition(topic, partitionId)).logEndOffset, 0L)

    val topicAndPartition = new TopicAndPartition("new-topic", 0)
    // new leader will be selected based on the LEO
    val kafkaConfig = servers(0).config
    val controllerContext = new ControllerContext(zkUtils)
    controllerContext.controllerChannelManager = controllerChannelManager
    val leader = LeaderUtils.newLeader(controllerContext, kafkaConfig, topicAndPartition, Seq(1, 2))
    assertEquals(leader, 1)

    servers(0).shutdown
    Thread.sleep(zookeeper.tickTime)
    waitUntilLeaderIsKnown(servers, topic, partitionId)
    val leaderAndIsr = zkUtils.getLeaderAndIsrForPartition(topic, partitionId)
    assertEquals(leaderAndIsr.get.leader, 1)
    servers(0).startup
  }

  def createControllerChannelManager: ControllerChannelManager = {
    val controllerId = 1
    val controllerConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(controllerId, zkConnect))
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = servers.map(s => new Broker(s.config.brokerId, "localhost", TestUtils.boundPort(s), listenerName, securityProtocol))
    val controllerContext = new ControllerContext(zkUtils)
    controllerContext.liveBrokers = brokers.toSet
    val metrics = new Metrics
    new ControllerChannelManager(controllerContext, controllerConfig, Time.SYSTEM, metrics)
  }
}

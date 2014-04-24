/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import junit.framework.Assert._
import kafka.utils.{ZkUtils, TestUtils}
import kafka.server.{KafkaServer, KafkaConfig}
import org.junit.Test
import kafka.common._
import kafka.producer.{ProducerConfig, Producer}
import java.util.Properties
import kafka.api._
import kafka.consumer.SimpleConsumer
import kafka.producer.KeyedMessage
import kafka.common.TopicAndPartition
import kafka.api.PartitionOffsetRequestInfo

class DeleteTopicTest extends JUnit3Suite with ZooKeeperTestHarness {

  @Test
  def testDeleteTopicWithAllAliveReplicas() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    verifyTopicDeletion(topic, servers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testResumeDeleteTopicWithRecoveredFollower() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // shut down one follower replica
    val leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    follower.shutdown()
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // check if all replicas but the one that is shut down has deleted the log
    TestUtils.waitUntilTrue(() =>
      servers.filter(s => s.config.brokerId != follower.config.brokerId)
        .foldLeft(true)((res, server) => res && server.getLogManager().getLog(topicAndPartition).isEmpty), "Replicas 0,1 have not deleted log.")
    // ensure topic deletion is halted
    TestUtils.waitUntilTrue(() => ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic)),
      "Admin path /admin/delete_topic/test path deleted even when a follower replica is down")
    // restart follower replica
    follower.startup()
    verifyTopicDeletion(topic, servers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testResumeDeleteTopicOnControllerFailover() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    val controllerId = ZkUtils.getController(zkClient)
    val controller = servers.filter(s => s.config.brokerId == controllerId).head
    var leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    follower.shutdown()

    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // shut down the controller to trigger controller failover during delete topic
    controller.shutdown()

    // ensure topic deletion is halted
    TestUtils.waitUntilTrue(() => ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic)),
      "Admin path /admin/delete_topic/test path deleted even when a replica is down")

    controller.startup()
    follower.startup()

    verifyTopicDeletion(topic, servers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testRequestHandlingDuringDeleteTopic() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // shut down one follower replica
    var leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    follower.shutdown()
    // test if produce requests are failed with UnknownTopicOrPartitionException during delete topic
    val props1 = new Properties()
    props1.put("metadata.broker.list", servers.map(s => s.config.hostName + ":" + s.config.port).mkString(","))
    props1.put("serializer.class", "kafka.serializer.StringEncoder")
    props1.put("request.required.acks", "1")
    val producerConfig1 = new ProducerConfig(props1)
    val producer1 = new Producer[String, String](producerConfig1)
    try {
      producer1.send(new KeyedMessage[String, String](topic, "test", "test1"))
      fail("Test should fail because the topic is being deleted")
    } catch {
      case e: FailedToSendMessageException =>
      case oe: Throwable => fail("fails with exception", oe)
    } finally {
      producer1.close()
    }
    // test if fetch requests fail during delete topic
    val availableServers: Seq[KafkaServer] = servers.filter(s => s.config.brokerId != follower.config.brokerId).toSeq
    availableServers.foreach {
      server =>
        val consumer = new SimpleConsumer(server.config.hostName, server.config.port, 1000000, 64 * 1024, "")
        val request = new FetchRequestBuilder()
          .clientId("test-client")
          .addFetch(topic, 0, 0, 10000)
          .build()
        val fetched = consumer.fetch(request)
        val fetchResponse = fetched.data(topicAndPartition)
        assertEquals("Fetch should fail with UnknownTopicOrPartitionCode", ErrorMapping.UnknownTopicOrPartitionCode, fetchResponse.error)
    }
    // test if offset requests fail during delete topic
    availableServers.foreach {
      server =>
        val consumer = new SimpleConsumer(server.config.hostName, server.config.port, 1000000, 64 * 1024, "")
        val offsetRequest = new OffsetRequest(Map(topicAndPartition -> new PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
        val offsetResponse = consumer.getOffsetsBefore(offsetRequest)
        val errorCode = offsetResponse.partitionErrorAndOffsets(topicAndPartition).error
        assertEquals("Offset request should fail with UnknownTopicOrPartitionCode", ErrorMapping.UnknownTopicOrPartitionCode, errorCode)
    }
    // restart follower replica
    follower.startup()
    verifyTopicDeletion(topic, availableServers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testPartitionReassignmentDuringDeleteTopic() {
    val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
    val topic = "test"
    val topicAndPartition = TopicAndPartition(topic, 0)
    val brokerConfigs = TestUtils.createBrokerConfigs(4)
    brokerConfigs.foreach(p => p.setProperty("delete.topic.enable", "true"))
    // create brokers
    val allServers = brokerConfigs.map(b => TestUtils.createServer(new KafkaConfig(b)))
    val servers = allServers.filter(s => expectedReplicaAssignment(0).contains(s.config.brokerId))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // wait until replica log is created on every broker
    TestUtils.waitUntilTrue(() => servers.foldLeft(true)((res, server) =>
      res && server.getLogManager().getLog(topicAndPartition).isDefined),
      "Replicas for topic test not created.")
    var leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    follower.shutdown()
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // start partition reassignment at the same time right after delete topic. In this case, reassignment will fail since
    // the topic is being deleted
    // reassign partition 0
    val oldAssignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, 0)
    val newReplicas = Seq(1, 2, 3)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment should fail for [test,0]", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
      val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient).mapValues(_.newReplicas);
      ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkClient, topicAndPartition, newReplicas,
        Map(topicAndPartition -> newReplicas), partitionsBeingReassigned) == ReassignmentFailed;
    }, "Partition reassignment shouldn't complete.")
    val controllerId = ZkUtils.getController(zkClient)
    val controller = servers.filter(s => s.config.brokerId == controllerId).head
    assertFalse("Partition reassignment should fail",
      controller.kafkaController.controllerContext.partitionsBeingReassigned.contains(topicAndPartition))
    val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, 0)
    assertEquals("Partition should not be reassigned to 0, 1, 2", oldAssignedReplicas, assignedReplicas)
    follower.startup()
    verifyTopicDeletion(topic, servers)
    allServers.foreach(_.shutdown())
  }

  @Test
  def testDeleteTopicDuringAddPartition() {
    val topic = "test"
    val servers = createTestTopicAndCluster(topic)
    var leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    val newPartition = TopicAndPartition(topic, 1)
    follower.shutdown()
    // add partitions to topic
    AdminUtils.addPartitions(zkClient, topic, 2, "0:1:2,0:1:2", false)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    follower.startup()
    // test if topic deletion is resumed
    verifyTopicDeletion(topic, servers)
    // verify that new partition doesn't exist on any broker either
    TestUtils.waitUntilTrue(() =>
      servers.foldLeft(true)((res, server) => res && server.getLogManager().getLog(newPartition).isEmpty),
      "Replica logs not for new partition [test,1] not deleted after delete topic is complete.")
    servers.foreach(_.shutdown())
  }

  @Test
  def testAddPartitionDuringDeleteTopic() {
    val topic = "test"
    val topicAndPartition = TopicAndPartition(topic, 0)
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // add partitions to topic
    val newPartition = TopicAndPartition(topic, 1)
    AdminUtils.addPartitions(zkClient, topic, 2, "0:1:2,0:1:2")
    verifyTopicDeletion(topic, servers)
    // verify that new partition doesn't exist on any broker either
    assertTrue("Replica logs not deleted after delete topic is complete",
      servers.foldLeft(true)((res, server) => res && server.getLogManager().getLog(newPartition).isEmpty))
    servers.foreach(_.shutdown())
  }

  @Test
  def testRecreateTopicAfterDeletion() {
    val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
    val topic = "test"
    val topicAndPartition = TopicAndPartition(topic, 0)
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    verifyTopicDeletion(topic, servers)
    // re-create topic on same replicas
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // wait until leader is elected
    val leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 1000)
    assertTrue("New leader should be elected after re-creating topic test", leaderIdOpt.isDefined)
    // check if all replica logs are created
    TestUtils.waitUntilTrue(() => servers.foldLeft(true)((res, server) => res && server.getLogManager().getLog(topicAndPartition).isDefined),
      "Replicas for topic test not created.")
    servers.foreach(_.shutdown())
  }

  @Test
  def testAutoCreateAfterDeleteTopic() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    verifyTopicDeletion(topic, servers)
    // test if first produce request after topic deletion auto creates the topic
    val props = new Properties()
    props.put("metadata.broker.list", servers.map(s => s.config.hostName + ":" + s.config.port).mkString(","))
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "sync")
    props.put("request.required.acks", "1")
    props.put("message.send.max.retries", "1")
    val producerConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](producerConfig)
    try {
      producer.send(new KeyedMessage[String, String](topic, "test", "test1"))
    } catch {
      case e: FailedToSendMessageException => fail("Topic should have been auto created")
      case oe: Throwable => fail("fails with exception", oe)
    }
    // test the topic path exists
    assertTrue("Topic not auto created", ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic)))
    // wait until leader is elected
    val leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 1000)
    assertTrue("New leader should be elected after re-creating topic test", leaderIdOpt.isDefined)
    servers.foreach(_.shutdown())
  }

  @Test
  def testDeleteNonExistingTopic() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, "test2")
    // verify delete topic path for test2 is removed from zookeeper
    verifyTopicDeletion("test2", servers)
    // verify that topic test is untouched
    TestUtils.waitUntilTrue(() => servers.foldLeft(true)((res, server) =>
      res && server.getLogManager().getLog(topicAndPartition).isDefined),
      "Replicas for topic test not created")
    // test the topic path exists
    assertTrue("Topic test mistakenly deleted", ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic)))
    // topic test should have a leader
    val leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 1000)
    assertTrue("Leader should exist for topic test", leaderIdOpt.isDefined)
    servers.foreach(_.shutdown())

  }

  private def createTestTopicAndCluster(topic: String): Seq[KafkaServer] = {
    val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
    val topicAndPartition = TopicAndPartition(topic, 0)
    val brokerConfigs = TestUtils.createBrokerConfigs(3)
    brokerConfigs.foreach(p => p.setProperty("delete.topic.enable", "true"))
    // create brokers
    val servers = brokerConfigs.map(b => TestUtils.createServer(new KafkaConfig(b)))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // wait until replica log is created on every broker
    TestUtils.waitUntilTrue(() => servers.foldLeft(true)((res, server) =>
      res && server.getLogManager().getLog(topicAndPartition).isDefined),
      "Replicas for topic test not created")
    servers
  }

  private def verifyTopicDeletion(topic: String, servers: Seq[KafkaServer]) {
    val topicAndPartition = TopicAndPartition(topic, 0)
    // wait until admin path for delete topic is deleted, signaling completion of topic deletion
    TestUtils.waitUntilTrue(() => !ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic)),
      "Admin path /admin/delete_topic/test path not deleted even after a replica is restarted")
    TestUtils.waitUntilTrue(() => !ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic)),
      "Topic path /brokers/topics/test not deleted after /admin/delete_topic/test path is deleted")
    // ensure that logs from all replicas are deleted if delete topic is marked successful in zookeeper
    assertTrue("Replica logs not deleted after delete topic is complete",
      servers.foldLeft(true)((res, server) => res && server.getLogManager().getLog(topicAndPartition).isEmpty))
  }
}
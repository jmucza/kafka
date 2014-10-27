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

namespace Kafka.Client.IntegrationTests
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;

    using Kafka.Client.Consumers;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration;
    using Kafka.Client.ZooKeeperIntegration.Cluster;
    using Kafka.Client.ZooKeeperIntegration.Entities;
    using Kafka.Client.ZooKeeperIntegration.Serialization;
    using Kafka.Client.ZooKeeperIntegration.Utils;

    using Microsoft.Win32;

    using NUnit.Framework;

    [TestFixture]
    public class ConsumerRebalancingTests : IntegrationFixtureBase
    {
        [Test]
        public void ConsumerPerformsRebalancingOnStart()
        {
            var config = this.ZooKeeperBasedConsumerConfig;
            using (var consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
                var client = this.GetZooKeeperClient(consumerConnector);

                client.DeleteRecursive("/consumers/group1");
                var topicCount = new Dictionary<string, int> { { "test", 1 } };
                consumerConnector.CreateMessageStreams(topicCount);
                WaitUntillIdle(client, 1000);
                IList<string> children = client.GetChildren("/consumers", false);
                Assert.That(children, Is.Not.Null.And.Not.Empty);
                Assert.That(children, Contains.Item("group1"));
                children = client.GetChildren("/consumers/group1", false);
                Assert.That(children, Is.Not.Null.And.Not.Empty);
                Assert.That(children, Contains.Item("ids"));
                Assert.That(children, Contains.Item("owners"));
                children = client.GetChildren("/consumers/group1/ids", false);
                Assert.That(children, Is.Not.Null.And.Not.Empty);
                string consumerId = children[0];
                children = client.GetChildren("/consumers/group1/owners", false);
                Assert.That(children, Is.Not.Null.And.Not.Empty);
                Assert.That(children.Count, Is.EqualTo(1));
                Assert.That(children, Contains.Item("test"));
                children = client.GetChildren("/consumers/group1/owners/test", false);
                Assert.That(children, Is.Not.Null.And.Not.Empty);

                string partId = children[0];
                var data = client.ReadData<string>("/consumers/group1/owners/test/" + partId);
                Assert.That(data, Is.Not.Null.And.Not.Empty);
                Assert.That(data, Contains.Substring(consumerId));
                data = client.ReadData<string>("/consumers/group1/ids/" + consumerId);
                Assert.That(data, Is.Not.Null.And.Not.Empty);
                var consumerRegistrationInfo = data.DeserializeAs<ConsumerRegistrationInfo>();
                Assert.That(consumerRegistrationInfo.Subscription["test"], Is.EqualTo(1));
            }

            using (var client = new ZooKeeperClient(config.ZooKeeper.ZkConnect, config.ZooKeeper.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                //// Should be created as ephemeral
                IList<string> children = client.GetChildren("/consumers/group1/ids");
                Assert.That(children, Is.Null.Or.Empty);
                //// Should be created as ephemeral
                children = client.GetChildren("/consumers/group1/owners/test");
                Assert.That(children, Is.Null.Or.Empty);
            }
        }

        [Test, Ignore("This test does not check if adding a new broker resulted in consumer rebalancing")]
        public void ConsumerPorformsRebalancingWhenNewBrokerIsAddedToTopic()
        {
            const string Topic = "test";
            const string ConsumerGroup = "group1";
			const int NewBrokerId = 2345;

            var config = this.ZooKeeperBasedConsumerConfig;
	        string brokerPath = ZooKeeperClient.DefaultBrokerIdsPath + "/" + NewBrokerId;
            string brokerTopicPath = ZooKeeperClient.DefaultBrokerTopicsPath + "/" + Topic + "/" + NewBrokerId;
            using (var consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
                var dirs = new ZKGroupTopicDirs(ConsumerGroup, Topic);

                var client = this.GetZooKeeperClient(consumerConnector);

                client.DeleteRecursive(dirs.ConsumerGroupDir);
                var topicCount = new Dictionary<string, int> { { Topic, 1 } };
                consumerConnector.CreateMessageStreams(topicCount);
                WaitUntillIdle(client, 1000);

                IList<string> children = client.GetChildren(dirs.ConsumerRegistryDir, false);
                string consumerId = children[0];

				// add new broker
	            var brokerRegistrationInfo = new BrokerRegistrationInfo(GetHost(), 9092);
                client.CreateEphemeral(brokerPath, brokerRegistrationInfo.SerializeAsJson());

                client.CreateEphemeral(brokerTopicPath, 1);
                WaitUntillIdle(client, 500);

                children = client.GetChildren(dirs.ConsumerOwnerDir, false);
                Assert.That(children.Count, Is.EqualTo(3));

                const string PartitionId = "0";
                Assert.That(children, Contains.Item(PartitionId));

                var data = client.ReadData<string>(dirs.GetConsumerOwnerPartitionDir(PartitionId));
                Assert.That(data, Is.Not.Null);
                Assert.That(data, Contains.Substring(consumerId));

                var topicRegistry = this.GetTopicRegistry(consumerConnector);
                Assert.That(topicRegistry, Is.Not.Null.And.Not.Empty);
                Assert.That(topicRegistry.Count, Is.EqualTo(1));
                var item = topicRegistry[Topic];
                Assert.That(item.Count, Is.EqualTo(3));
            }
        }

		[Test, Ignore("This test does not check if removing a new broker resulted in consumer rebalancing")]
		public void ConsumerPorformsRebalancingWhenBrokerIsRemovedFromTopic()
        {
			const string Topic = "test";
			const string ConsumerGroup = "group1";
			const int RemovedBrokerId = 2345;
			
			var config = this.ZooKeeperBasedConsumerConfig;

	        string brokerPath = ZooKeeperClient.DefaultBrokerIdsPath + "/" + RemovedBrokerId;
            string brokerTopicPath = ZooKeeperClient.DefaultBrokerTopicsPath + "/" + Topic + "/" + RemovedBrokerId;
            using (var consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
	            var dirs = new ZKGroupTopicDirs(ConsumerGroup, Topic);

                var client = this.GetZooKeeperClient(consumerConnector);
                
                client.DeleteRecursive(dirs.ConsumerGroupDir);
                var topicCount = new Dictionary<string, int> { { Topic, 1 } };
                consumerConnector.CreateMessageStreams(topicCount);
                WaitUntillIdle(client, 1000);

	            var brokerRegistrationInfo = new BrokerRegistrationInfo(GetHost(), 9092);
                client.CreateEphemeral(brokerPath, brokerRegistrationInfo.SerializeAsJson());

                client.CreateEphemeral(brokerTopicPath, 1);
                WaitUntillIdle(client, 1000);
                client.DeleteRecursive(brokerTopicPath);
                WaitUntillIdle(client, 1000);

                IList<string> children = client.GetChildren(dirs.ConsumerOwnerDir, false);
                Assert.That(children.Count, Is.EqualTo(3));
                Assert.That(children, Has.None.EqualTo(string.Format("{0}-0", RemovedBrokerId)));
                var topicRegistry = this.GetTopicRegistry(consumerConnector);
                Assert.That(topicRegistry, Is.Not.Null.And.Not.Empty);
                Assert.That(topicRegistry.Count, Is.EqualTo(1));
                var item = topicRegistry[Topic];
                Assert.That(item.Count, Is.EqualTo(3));
                Assert.That(item.Where(x => x.Value.BrokerId == RemovedBrokerId).Count(), Is.EqualTo(0));
            }
        }

	    [Test]
        public void ConsumerPerformsRebalancingWhenNewConsumerIsAddedAndTheyDividePartitions()
        {
            var config = this.ZooKeeperBasedConsumerConfig;
            using (var consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
                var client = this.GetZooKeeperClient(consumerConnector);

                client.DeleteRecursive("/consumers/group1");
                var topicCount = new Dictionary<string, int> { { "test", 1 } };
                consumerConnector.CreateMessageStreams(topicCount);
                WaitUntillIdle(client, 1000);
                using (var consumerConnector2 = new ZookeeperConsumerConnector(config, true))
                {
                    consumerConnector2.CreateMessageStreams(topicCount);
                    WaitUntillIdle(client, 1000);
                    var ids = client.GetChildren("/consumers/group1/ids", false).ToList();

                    Assert.That(ids, Is.Not.Null.And.Not.Empty);
                    Assert.That(ids.Count, Is.EqualTo(2));

					var owners = client.GetTopicPartitionOwners("group1", "test");
                    Assert.That(owners, Is.Not.Null.And.Not.Empty);

                    var consumers = owners.Values.Distinct().ToList();
                    Assert.That(consumers.Count(), Is.EqualTo(ids.Count));
                    foreach (var id in ids)
                    {
                        Assert.IsTrue(consumers.Exists(consumer => consumer.StartsWith(id)));
                    }
                }
            }
        }

        [Test]
        public void ConsumerPerformsRebalancingWhenConsumerIsRemovedAndTakesItsPartitions()
        {
            var config = this.ZooKeeperBasedConsumerConfig;
            string basePath = "/consumers/" + config.GroupId;
            IList<string> ids;
            IDictionary<int, string> owners;
            List<string> consumers;
            using (var consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
                var client = this.GetZooKeeperClient(consumerConnector);

                client.DeleteRecursive("/consumers/group1");
                var topicCount = new Dictionary<string, int> { { "test", 1 } };
                consumerConnector.CreateMessageStreams(topicCount);
                WaitUntillIdle(client, 1000);
                using (var consumerConnector2 = new ZookeeperConsumerConnector(config, true))
                {
                    consumerConnector2.CreateMessageStreams(topicCount);
                    WaitUntillIdle(client, 1000);
                    ids = client.GetChildren("/consumers/group1/ids", false).ToList();
                    Assert.That(ids, Is.Not.Null.And.Not.Empty);
                    Assert.That(ids.Count, Is.EqualTo(2));

					owners = client.GetTopicPartitionOwners("group1", "test");
                    consumers = owners.Values.Distinct().ToList();
                    Assert.That(owners, Is.Not.Null.And.Not.Empty);
                    Assert.That(consumers.Count(), Is.EqualTo(ids.Count));
                }

                WaitUntillIdle(client, 1000);
                ids = client.GetChildren("/consumers/group1/ids", false).ToList();

                Assert.That(ids, Is.Not.Null.And.Not.Empty);
                Assert.That(ids.Count, Is.EqualTo(1));

				owners = client.GetTopicPartitionOwners("group1", "test");
                consumers = owners.Values.Distinct().ToList();

                Assert.That(owners, Is.Not.Null.And.Not.Empty);
                Assert.That(consumers.Count(), Is.EqualTo(ids.Count));

                foreach (var id in ids)
                {
                    Assert.IsTrue(consumers.Exists(consumer => consumer.StartsWith(id)));
                }
                //var data1 = client.ReadData<string>("/consumers/group1/owners/test/" + owners[0], false);
                //var data2 = client.ReadData<string>("/consumers/group1/owners/test/" + owners[1], false);

                //Assert.That(data1, Is.Not.Null.And.Not.Empty);
                //Assert.That(data2, Is.Not.Null.And.Not.Empty);
                //Assert.That(data1, Is.EqualTo(data2));
                //Assert.That(data1, Is.StringStarting(ids[0]));
            }
        }

        private IZooKeeperClient GetZooKeeperClient(ZookeeperConsumerConnector consumerConnector)
        {
            var client = ReflectionHelper.GetInstanceField<ZooKeeperClient>("zkClient", consumerConnector);
            Assert.IsNotNull(client);

            return client;
        }

        private IDictionary<string, IDictionary<Partition, PartitionTopicInfo>> GetTopicRegistry(ZookeeperConsumerConnector consumerConnector)
        {
            return ReflectionHelper.GetInstanceField<IDictionary<string, IDictionary<Partition, PartitionTopicInfo>>>( "topicRegistry", consumerConnector);
        }
	
		private static string GetHost()
		{
			return Dns.GetHostAddresses(Dns.GetHostName()).First(a => a.AddressFamily == AddressFamily.InterNetwork).ToString();
		}
	}
}

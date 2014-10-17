﻿/**
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

using System.ComponentModel;

using Kafka.Client.Utils;
using Kafka.Client.ZooKeeperIntegration;

namespace Kafka.Client.IntegrationTests
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Requests;
    using Kafka.Client.Serialization;
    using Kafka.Client.ZooKeeperIntegration.Events;
    using Kafka.Client.ZooKeeperIntegration.Partitioning;

    using NUnit.Framework;

	[TestFixture]
    public class ZooKeeperAwareProducerTests : IntegrationFixtureBase
    {
        /// <summary>
        /// Maximum amount of time to wait trying to get a specific test message from Kafka server (in miliseconds)
        /// </summary>
        private readonly int maxTestWaitTimeInMiliseconds = 5000;

        [Test]
        public void ZkAwareProducerSends1Message()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;

            int totalWaitTimeInMiliseconds = 0;
            int waitSingle = 100;
            var originalMessage = new Message(Encoding.UTF8.GetBytes("TestData2"));
            var topic = CurrentTestTopic;

            var multipleBrokersHelper = new TestMultipleBrokersHelper(topic);
            multipleBrokersHelper.GetCurrentOffsets(new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 });

            var mockPartitioner = new MockAlwaysZeroPartitioner();
            using (var producer = new Producer<string, Message>(prodConfig, mockPartitioner, new DefaultEncoder()))
            {
                var producerData = new ProducerData<string, Message>(
                    topic, "somekey", new List<Message> { originalMessage });
                producer.Send(producerData);

                while (!multipleBrokersHelper.CheckIfAnyBrokerHasChanged(new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 }))
                {
                    totalWaitTimeInMiliseconds += waitSingle;
                    Thread.Sleep(waitSingle);
                    if (totalWaitTimeInMiliseconds > this.maxTestWaitTimeInMiliseconds)
                    {
                        Assert.Fail("None of the brokers changed their offset after sending a message");
                    }
                }

                totalWaitTimeInMiliseconds = 0;

                var consumerConfig = new ConsumerConfiguration(
                    multipleBrokersHelper.BrokerThatHasChanged.Host,
                    multipleBrokersHelper.BrokerThatHasChanged.Port);
                IConsumer consumer = new Consumer(consumerConfig);
                var request = new FetchRequest(topic, multipleBrokersHelper.PartitionThatHasChanged, multipleBrokersHelper.OffsetFromBeforeTheChange);

                BufferedMessageSet response;

                while (true)
                {
                    Thread.Sleep(waitSingle);
                    response = consumer.Fetch(request);
                    if (response != null & response.Messages.Count() > 0)
                    {
                        break;
                    }

                    totalWaitTimeInMiliseconds += waitSingle;
                    if (totalWaitTimeInMiliseconds >= this.maxTestWaitTimeInMiliseconds)
                    {
                        break;
                    }
                }

                Assert.NotNull(response);
                Assert.AreEqual(1, response.Messages.Count());
                Assert.AreEqual(originalMessage.ToString(), response.Messages.First().ToString());
            }
        }

        [Test]
        public void ZkAwareProducerSends3Messages()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;
            int totalWaitTimeInMiliseconds = 0;
            int waitSingle = 100;
            var originalMessage1 = new Message(Encoding.UTF8.GetBytes("TestData1"));
            var originalMessage2 = new Message(Encoding.UTF8.GetBytes("TestData2"));
            var originalMessage3 = new Message(Encoding.UTF8.GetBytes("TestData3"));
            var originalMessageList = new List<Message> { originalMessage1, originalMessage2, originalMessage3 };

            var multipleBrokersHelper = new TestMultipleBrokersHelper(CurrentTestTopic);
            multipleBrokersHelper.GetCurrentOffsets(new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 });

            var mockPartitioner = new MockAlwaysZeroPartitioner();
            using (var producer = new Producer<string, Message>(prodConfig, mockPartitioner, new DefaultEncoder()))
            {
                var producerData = new ProducerData<string, Message>(CurrentTestTopic, "somekey", originalMessageList);
                producer.Send(producerData);

                while (!multipleBrokersHelper.CheckIfAnyBrokerHasChanged(new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 }))
                {
                    totalWaitTimeInMiliseconds += waitSingle;
                    Thread.Sleep(waitSingle);
                    if (totalWaitTimeInMiliseconds > this.maxTestWaitTimeInMiliseconds)
                    {
                        Assert.Fail("None of the brokers changed their offset after sending a message");
                    }
                }

                totalWaitTimeInMiliseconds = 0;

                var consumerConfig = new ConsumerConfiguration(
                    multipleBrokersHelper.BrokerThatHasChanged.Host,
                    multipleBrokersHelper.BrokerThatHasChanged.Port);
                IConsumer consumer = new Consumer(consumerConfig);
                var request = new FetchRequest(CurrentTestTopic, 0, multipleBrokersHelper.OffsetFromBeforeTheChange);
                BufferedMessageSet response;

                while (true)
                {
                    Thread.Sleep(waitSingle);
                    response = consumer.Fetch(request);
                    if (response != null && response.Messages.Count() > 2)
                    {
                        break;
                    }

                    totalWaitTimeInMiliseconds += waitSingle;
                    if (totalWaitTimeInMiliseconds >= this.maxTestWaitTimeInMiliseconds)
                    {
                        break;
                    }
                }

                Assert.NotNull(response);
                Assert.AreEqual(3, response.Messages.Count());
                Assert.AreEqual(originalMessage1.ToString(), response.Messages.First().ToString());
                Assert.AreEqual(originalMessage2.ToString(), response.Messages.Skip(1).First().ToString());
                Assert.AreEqual(originalMessage3.ToString(), response.Messages.Skip(2).First().ToString());
            }
        }

        [Test]
        public void ZkAwareProducerSends1MessageUsingNotDefaultEncoder()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;

            int totalWaitTimeInMiliseconds = 0;
            int waitSingle = 100;
            string originalMessage = "TestData";

            var multipleBrokersHelper = new TestMultipleBrokersHelper(CurrentTestTopic);
            multipleBrokersHelper.GetCurrentOffsets(new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 });

            var mockPartitioner = new MockAlwaysZeroPartitioner();
            using (var producer = new Producer<string, string>(prodConfig, mockPartitioner, new StringEncoder(), null))
            {
                var producerData = new ProducerData<string, string>(
                    CurrentTestTopic, "somekey", new List<string> { originalMessage });
                producer.Send(producerData);

                while (!multipleBrokersHelper.CheckIfAnyBrokerHasChanged(new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 }))
                {
                    totalWaitTimeInMiliseconds += waitSingle;
                    Thread.Sleep(waitSingle);
                    if (totalWaitTimeInMiliseconds > this.maxTestWaitTimeInMiliseconds)
                    {
                        Assert.Fail("None of the brokers changed their offset after sending a message");
                    }
                }

                totalWaitTimeInMiliseconds = 0;

                var consumerConfig = new ConsumerConfiguration(
                    multipleBrokersHelper.BrokerThatHasChanged.Host,
                    multipleBrokersHelper.BrokerThatHasChanged.Port);
                IConsumer consumer = new Consumer(consumerConfig);
                var request = new FetchRequest(CurrentTestTopic, 0, multipleBrokersHelper.OffsetFromBeforeTheChange);
                BufferedMessageSet response;

                while (true)
                {
                    Thread.Sleep(waitSingle);
                    response = consumer.Fetch(request);
                    if (response != null && response.Messages.Count() > 0)
                    {
                        break;
                    }

                    totalWaitTimeInMiliseconds += waitSingle;
                    if (totalWaitTimeInMiliseconds >= this.maxTestWaitTimeInMiliseconds)
                    {
                        break;
                    }
                }

                Assert.NotNull(response);
                Assert.AreEqual(1, response.Messages.Count());
                Assert.AreEqual(originalMessage, Encoding.UTF8.GetString(response.Messages.First().Payload));
            }
        }

        [Test]
        public void ZkAwareProducerSendsLotsOfMessagesAndSessionCreatedHandlerInvokedInTheBackgroundShouldNotThrowException()
        {
            var prodConfig = this.ZooKeeperBasedAsyncProdConfig;
            var originalMessage = new Message(Encoding.UTF8.GetBytes("TestData1"));
            var originalMessageList = new List<Message> { originalMessage };
            int numberOfPackagesToSend = 500;
            int runBackgroundWorkerAfterNIterations = 100;

            var mockPartitioner = new MockAlwaysZeroPartitioner();
            using (var producer = new Producer<string, Message>(prodConfig, mockPartitioner, new DefaultEncoder()))
            {
                BackgroundWorker bw = new BackgroundWorker();
                bw.WorkerSupportsCancellation = true;
                bw.DoWork += new DoWorkEventHandler(ZkAwareProducerSendsLotsOfMessagesAndSessionCreatedHandlerInvokedInTheBackgroundShouldNotThrowException_DoWork);
                
                for (int i = 0; i < numberOfPackagesToSend; i++)
                {
                    if (i == runBackgroundWorkerAfterNIterations)
                    {
                        bw.RunWorkerAsync(producer);
                    }
                    var producerData = new ProducerData<string, Message>(CurrentTestTopic, "somekey",
                                                                         originalMessageList);
                    producer.Send(producerData);
                }

                bw.CancelAsync();
            }
        }

        void ZkAwareProducerSendsLotsOfMessagesAndSessionCreatedHandlerInvokedInTheBackgroundShouldNotThrowException_DoWork(object sender, DoWorkEventArgs e)
        {
            BackgroundWorker worker = sender as BackgroundWorker;
            while (true)
            {
                if (worker.CancellationPending)
                {
                    e.Cancel = true;
                    break;
                }
                Producer<string, Message> producer = e.Argument as Producer<string, Message>;
                IBrokerPartitionInfo brokerPartitionInfo =
                    ReflectionHelper.GetInstanceField<IBrokerPartitionInfo>("brokerPartitionInfo", producer);
                IZooKeeperClient zkclient = ReflectionHelper.GetInstanceField<IZooKeeperClient>(
                        "zkclient", brokerPartitionInfo);
                var sessionCreatedHandler = ReflectionHelper.GetInstanceField<ZooKeeperEventHandler<ZooKeeperSessionCreatedEventArgs>>("sessionCreatedHandlers", zkclient);
                sessionCreatedHandler.Invoke(ZooKeeperSessionCreatedEventArgs.Empty);
                Thread.Sleep(1000);
            }
        }
    }
}

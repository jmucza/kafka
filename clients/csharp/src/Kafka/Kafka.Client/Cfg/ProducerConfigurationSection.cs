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

namespace Kafka.Client.Cfg
{
    using System.Configuration;
    using Kafka.Client.Producers;
    using System.Xml.Linq;

    using Kafka.Client.ZooKeeperIntegration.Configuration;

	public class ProducerConfigurationSection : ConfigurationSection
    {
        [ConfigurationProperty(
            "type",
            DefaultValue = ProducerConfiguration.DefaultProducerType,
            IsRequired = false)]
        public ProducerTypes ProducerType
        {
            get
            {
                return (ProducerTypes)this["type"];
            }
        }

        [ConfigurationProperty(
            "bufferSize",
            DefaultValue = SyncProducerConfiguration.DefaultBufferSize,
            IsRequired = false)]
        public int BufferSize
        {
            get
            {
                return (int)this["bufferSize"];
            }
        }

        [ConfigurationProperty(
            "connectionTimeout",
            DefaultValue = SyncProducerConfiguration.DefaultConnectTimeout,
            IsRequired = false)]
        public int ConnectionTimeout
        {
            get
            {
                return (int)this["connectionTimeout"];
            }
        }

        [ConfigurationProperty(
            "socketTimeout",
            DefaultValue = SyncProducerConfiguration.DefaultSocketTimeout,
            IsRequired = false)]
        public int SocketTimeout
        {
            get
            {
                return (int)this["socketTimeout"];
            }
        }

        [ConfigurationProperty(
            "maxMessageSize",
            DefaultValue = SyncProducerConfiguration.DefaultMaxMessageSize,
            IsRequired = false)]
        public int MaxMessageSize
        {
            get
            {
                return (int)this["maxMessageSize"];
            }
        }

        [ConfigurationProperty("brokers", IsRequired = false, IsDefaultCollection = true)]
        [ConfigurationCollection(typeof(BrokerConfigurationElementCollection),
            AddItemName = "add",
            ClearItemsName = "clear",
            RemoveItemName = "remove")]
        public BrokerConfigurationElementCollection Brokers
        {
            get
            {
                return (BrokerConfigurationElementCollection)this["brokers"];
            }
        }

        [ConfigurationProperty("zookeeper", IsRequired = false, DefaultValue = null)]
        public ZooKeeperConfigurationElement ZooKeeperServers
        {
            get 
            { 
                 return (ZooKeeperConfigurationElement)this["zookeeper"];
            }
        }

        [ConfigurationProperty(
            "serializer",
            DefaultValue = AsyncProducerConfiguration.DefaultSerializerClass,
            IsRequired = false)]
        public string Serializer
        {
            get
            {
                return (string)this["serializer"];
            }
        }

        [ConfigurationProperty(
            "partitioner",
            DefaultValue = ProducerConfiguration.DefaultPartitioner,
            IsRequired = false)]
        public string Partitioner
        {
            get
            {
                return (string)this["partitioner"];
            }
        }

        [ConfigurationProperty(
            "reconnectInterval",
            DefaultValue = SyncProducerConfiguration.DefaultReconnectInterval,
            IsRequired = false)]
        public int ReconnectInterval
        {
            get
            {
                return (int)this["reconnectInterval"];
            }
        }

        [ConfigurationProperty(
            "reconnectTimeInterval",
            DefaultValue = SyncProducerConfiguration.DefaultReconnectTimeInterval,
            IsRequired = false)]
        public int ReconnectTimeInterval
        {
            get
            {
                return (int)this["reconnectTimeInterval"];
            }
        }

        public static ProducerConfigurationSection FromXml(XElement xml)
        {
            var config = new ProducerConfigurationSection();
            config.DeserializeSection(xml.CreateReader());
            return config;
        }

    }
}

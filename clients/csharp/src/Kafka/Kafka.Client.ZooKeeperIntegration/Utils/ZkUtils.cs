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

namespace Kafka.Client.ZooKeeperIntegration.Utils
{
	using System.Collections.Generic;
	using System.Globalization;
	using System.Linq;
	using System.Reflection;

	using Kafka.Client.ZooKeeperIntegration.Cluster;
	using Kafka.Client.ZooKeeperIntegration.Entities;
	using Kafka.Client.ZooKeeperIntegration.Serialization;

	using log4net;

	using ZooKeeperNet;

	public class ZkUtils
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

		public static void UpdatePersistentPath(IZooKeeperClient zkClient, string path, string data)
        {
            try
            {
                zkClient.WriteData(path, data);
            }
            catch (KeeperException.NoNodeException)
            {
                CreateParentPath(zkClient, path);

                try
                {
                    zkClient.CreatePersistent(path, data);
                }
                catch (KeeperException.NodeExistsException)
                {
                    zkClient.WriteData(path, data);
                }
            }
        }

        internal static void CreateParentPath(IZooKeeperClient zkClient, string path)
        {
            string parentDir = path.Substring(0, path.LastIndexOf('/'));
            if (parentDir.Length != 0)
            {
                zkClient.CreatePersistent(parentDir, true);
            }
        }

		public static void DeletePath(IZooKeeperClient zkClient, string path)
        {
            try
            {
                zkClient.Delete(path);
            }
            catch (KeeperException.NoNodeException)
            {
                Logger.InfoFormat(CultureInfo.CurrentCulture, "{0} deleted during connection loss; this is ok", path);
            }
        }

		public static IDictionary<string, IList<string>> GetPartitionsForTopics(IZooKeeperClient zkClient, IEnumerable<string> topics)
        {
            var result = new Dictionary<string, IList<string>>();
            foreach (string topic in topics)
            {
	            var topicDirs = new ZkTopicDirs(topic);

				var partitions = GetPartitionsForTopic(zkClient, topic);

				var partList = new List<string>();
				foreach (var partition in partitions)
				{
					var partitionState = zkClient.ReadData<string>(topicDirs.GetPartitionStateDir(partition));
					var info = partitionState.DeserializeAs<PartitionStateInfo>();
					// for some misterious reason leader and partition id are grouped this way instead of tuple or strong type
					partList.Add(info.Leader + "-" + partition);
					//partList.Add(partition);
				}

				partList.Sort();
				result.Add(topic, partList);
            }

            return result;
        }

		public static IEnumerable<int> GetPartitionsForTopic(IZooKeeperClient zkClient, string topic)
		{
			var dirs = new ZkTopicDirs(topic);

			return zkClient.GetChildrenParentMayNotExist(dirs.TopicPartitionsDir).Select(int.Parse);
		}

		public static IDictionary<int, string> GetTopicPartitionOwners(
			IZooKeeperClient zkClient,
			string consumerGroup,
			string topic)
		{

			var partitions = GetPartitionsForTopic(zkClient, topic).ToList();

			var result = new Dictionary<int, string>(partitions.Count);
			var dirs = new ZKGroupTopicDirs(consumerGroup, topic);
			foreach (var partition in partitions)
			{
				var partitionOwner = zkClient.ReadData<string>(dirs.ConsumerOwnerDir + "/" + partition);
				result.Add(partition, partitionOwner);
			}

			return result;
		}

		public static IDictionary<int, BrokerRegistrationInfo> GetBrokerRegistrationInfos(IZooKeeperClient zkClient)
		{
			var result = new Dictionary<int, BrokerRegistrationInfo>();
			var brokerIds = zkClient.GetChildrenParentMayNotExist(ZooKeeperClient.DefaultBrokerIdsPath).Select(int.Parse);
			
			foreach (var brokerId in brokerIds)
			{
				var brokerPath = ZooKeeperClient.DefaultBrokerIdsPath + "/" + brokerId;
				var infoString = zkClient.ReadData<string>(brokerPath, true);

				if (!string.IsNullOrEmpty(infoString))
				{
					result.Add(brokerId, infoString.DeserializeAs<BrokerRegistrationInfo>());
				}
			}

			return result;
		}

		public static void CreateEphemeralPathExpectConflict(IZooKeeperClient zkClient, string path, string data)
        {
            try
            {
                CreateEphemeralPath(zkClient, path, data);
            }
            catch (KeeperException.NodeExistsException)
            {
                string storedData;
                try
                {
                    storedData = zkClient.ReadData<string>(path);
                }
                catch (KeeperException.NoNodeException)
                {
                    // the node disappeared; treat as if node existed and let caller handles this
                    throw;
                }

                if (storedData == null || storedData != data)
                {
                    Logger.InfoFormat(CultureInfo.CurrentCulture, "conflict in {0} data: {1} stored data: {2}", path, data, storedData);
                    throw;
                }
                else
                {
                    // otherwise, the creation succeeded, return normally
                    Logger.InfoFormat(CultureInfo.CurrentCulture, "{0} exits with value {1} during connection loss; this is ok", path, data);
                }
            }
        }

        internal static void CreateEphemeralPath(IZooKeeperClient zkClient, string path, string data)
        {
            try
            {
                zkClient.CreateEphemeral(path, data);
            }
            catch (KeeperException.NoNodeException)
            {
                ZkUtils.CreateParentPath(zkClient, path);
                zkClient.CreateEphemeral(path, data);
            }
        }
    }
}

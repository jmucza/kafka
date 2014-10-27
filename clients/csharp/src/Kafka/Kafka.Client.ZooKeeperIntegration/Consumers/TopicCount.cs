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

namespace Kafka.Client.ZooKeeperIntegration.Consumers
{
	using System;
	using System.Collections.Generic;
	using System.Reflection;
	using System.Text;

	using Kafka.Client.Log;
	using Kafka.Client.ZooKeeperIntegration.Entities;
	using Kafka.Client.ZooKeeperIntegration.Serialization;

	public class TopicCount
    {
		private static readonly ILogger Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

		public IDictionary<string, int> TopicCountMap { get; private set; }
        private readonly string consumerIdString;

        public TopicCount(string consumerIdString, IDictionary<string, int> topicCountMap)
        {
            this.TopicCountMap = topicCountMap;
            this.consumerIdString = consumerIdString;
        }

        public static TopicCount ConstructTopicCount(string consumerIdString, string json)
        {
            Dictionary<string, int> result = null;
            
            try
            {
	            var registrationInfo = json.DeserializeAs<ConsumerRegistrationInfo>();

				return new TopicCount(consumerIdString, registrationInfo.Subscription);

            }
            catch (Exception ex)
            {
	            Logger.Error(string.Format("error parsing consumer json string {0}", json), ex);
            }

            return new TopicCount(consumerIdString, result);
        }

        public IDictionary<string, IList<string>> GetConsumerThreadIdsPerTopic()
        {
            var result = new Dictionary<string, IList<string>>();
            foreach (KeyValuePair<string, int> item in this.TopicCountMap)
            {
                var consumerSet = new List<string>();
                for (int i = 0; i < item.Value; i++)
                {
                    consumerSet.Add(this.consumerIdString + "-" + i);
                }

                result.Add(item.Key, consumerSet);
            }

            return result;
        }

        public override bool Equals(object obj)
        {
            var o = obj as TopicCount;
            if (o != null)
            {
                return this.consumerIdString == o.consumerIdString && this.TopicCountMap == o.TopicCountMap;
            }

            return false;
        }

        /*
         return json of
         { "topic1" : 4,
           "topic2" : 4
         }
        */
        public string ToJsonString()
        {
            var sb = new StringBuilder();
            sb.Append("{ ");
            int i = 0;
            foreach (KeyValuePair<string, int> entry in this.TopicCountMap)
            {
                if (i > 0)
                {
                    sb.Append(",");
                }

                sb.Append("\"" + entry.Key + "\": " + entry.Value);
                i++;
            }

            sb.Append(" }");
            return sb.ToString();
        }
    }
}

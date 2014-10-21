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

using System.Collections.Generic;

namespace Kafka.Client.ZooKeeperIntegration.Entities
{
	/// <summary>
	/// Consumer registration info (Kafka 0.8.1)
	/// </summary>
	public class ConsumerRegistrationInfo
	{
		public const int DefaultVersion = 1;

		public int Version { get; set; }

		public string Pattern { get; set; }

		public IDictionary<string, int> Subscription { get; set; }

		public ConsumerRegistrationInfo(int version, string pattern, IDictionary<string, int> subscription)
		{
			this.Version = version;
			this.Pattern = pattern;
			this.Subscription = subscription;
		}

		public ConsumerRegistrationInfo(IDictionary<string, int> subscription)
			: this(DefaultVersion, ConsumerPattern.WhiteList, subscription)
		{
		}
	}

	public static class ConsumerPattern
	{
		public const string Static = "static";

		public const string WhiteList = "white_list";

		public const string BlackList = "black_list";
	}
}

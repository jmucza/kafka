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

namespace Kafka.Client.ZooKeeperIntegration.Entities
{
	using System;
	using System.Globalization;

	using Kafka.Client.Utils;

	using Newtonsoft.Json;

	/// <summary>
	/// Broker registration info (Kafka 0.8.1)
	/// </summary>
	public class BrokerRegistrationInfo
	{
		public const int DefaultVersion = 1;
		public const int DefaultJmxPort = -1;

		[JsonProperty(PropertyName = "version")]
		public int Version { get; set; }

		[JsonProperty(PropertyName = "host")]
		public string Host { get; set; }

		[JsonProperty(PropertyName = "port")]
		public int Port { get; set; }

		[JsonProperty(PropertyName = "jmx_port")]
		public int JmxPort { get; set; }

		[JsonProperty(PropertyName = "timestamp")]
		public string Timestamp { get; set; }

		[JsonConstructor]
		public BrokerRegistrationInfo(int version, string host, int port, int jmxPort, string timestamp)
		{
			this.Version = version;
			this.Host = host;
			this.Port = port;
			this.JmxPort = jmxPort;
			this.Timestamp = timestamp;
		}

		public BrokerRegistrationInfo(string host, int port)
			: this(DefaultVersion, host, port, DefaultJmxPort, DateTimeExtensions.CurrentTimeMillis().ToString(CultureInfo.InvariantCulture))
		{ }
	}
}

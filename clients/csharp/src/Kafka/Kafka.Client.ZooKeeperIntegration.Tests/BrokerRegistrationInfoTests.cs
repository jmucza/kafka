namespace Kafka.Client.ZooKeeperIntegration.Tests
{
	using Kafka.Client.ZooKeeperIntegration.Entities;

	using NUnit.Framework;

	[TestFixture]
	public class BrokerInfoTests
	{
		[Test]
		public void CanDeserializeFromJson()
		{
			const string Serialized = "{\"jmx_port\":-1,\"timestamp\":\"1413451093430\",\"host\":\"10.253.129.69\",\"version\":1,\"port\":9094}";
			var brokerInfo = Newtonsoft.Json.JsonConvert.DeserializeObject<BrokerRegistrationInfo>(Serialized);
			Assert.NotNull(brokerInfo);

			Assert.AreEqual("10.253.129.69", brokerInfo.Host);
			Assert.AreEqual(9094, brokerInfo.Port);
		}
	}
}

namespace Kafka.Client.ZooKeeperIntegration.Tests
{
	using Kafka.Client.ZooKeeperIntegration.Entities;

	using NUnit.Framework;

	[TestFixture]
	public class PartitionStateInfoTests
	{
		[Test]
		public void CanDeserializeFromJson()
		{
			const string Serialized = "{\"version\": 1,\"isr\": [0,1],\"leader\": 0,\"controller_epoch\": 1,\"leader_epoch\": 0}";
			var partitionStateInfo = Newtonsoft.Json.JsonConvert.DeserializeObject<PartitionStateInfo>(Serialized);
			Assert.NotNull(partitionStateInfo);

			Assert.AreEqual(2, partitionStateInfo.Isr.Count);
			Assert.AreEqual(0, partitionStateInfo.Leader);
		}
	}
}

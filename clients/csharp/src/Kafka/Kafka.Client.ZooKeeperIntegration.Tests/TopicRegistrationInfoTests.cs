namespace Kafka.Client.ZooKeeperIntegration.Tests
{
	using Kafka.Client.ZooKeeperIntegration.Entities;

	using NUnit.Framework;

	[TestFixture]
	public class TopicRegistrationInfoTests
	{
		[Test]
		public void CanDeserializeFromJson()
		{
			const string Serialized = "{\"version\":1,\"partitions\":{\"2\":[2],\"1\":[1],\"0\":[0]}}";
			var topicRegistrationInfo = Newtonsoft.Json.JsonConvert.DeserializeObject<TopicRegistrationInfo>(Serialized);
			Assert.NotNull(topicRegistrationInfo);

			Assert.AreEqual(3, topicRegistrationInfo.Partitions.Count);
			foreach (var isr in topicRegistrationInfo.Partitions.Values)
			{
				Assert.AreEqual(1, isr.Count);
			}
		}
	}
}
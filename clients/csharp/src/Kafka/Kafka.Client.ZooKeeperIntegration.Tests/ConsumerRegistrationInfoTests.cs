namespace Kafka.Client.ZooKeeperIntegration.Tests
{
	using Kafka.Client.ZooKeeperIntegration.Entities;

	using NUnit.Framework;

	[TestFixture]
	public class ConsumerRegistrationInfoTests
	{
		[Test]
		public void CanDeserializeFromJson()
		{
			const string Serialized = "{\"version\": 1,\"pattern\": \"static\",\"subscription\": {\"topic1\": 1, \"topic2\": 2}}";
			var consumerRegistrationInfo = Newtonsoft.Json.JsonConvert.DeserializeObject<ConsumerRegistrationInfo>(Serialized);
			Assert.NotNull(consumerRegistrationInfo);

			Assert.IsTrue(consumerRegistrationInfo.Subscription.ContainsKey("topic1"));
			Assert.IsTrue(consumerRegistrationInfo.Subscription.ContainsKey("topic2"));
		}
	}
}

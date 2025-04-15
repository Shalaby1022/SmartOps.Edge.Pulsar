using DotPulsar;

namespace SmartOps.Edge.Pulsar.BaseClasses.Models
{
	public class ConsumerData
	{
		/// <summary>
		/// Gets or sets the URL of the messaging service to connect to.
		/// </summary>
		public string ServiceUrl { get; set; } = "pulsar://localhost:6650";

		/// <summary>
		/// Gets or sets the name of the subscription used for message consumption.
		/// </summary>
		public string SubscriptionName { get; set; } = "DefaultSubscription";

		/// <summary>
		/// Gets or sets the fully qualified topic name to consume from (e.g., "persistent://public/default/test-topic-1").
		/// </summary>
		public string TopicName { get; set; } = "persistent://public/default/test-topic-1";

		/// <summary>
		/// Gets or sets the subscription type for message consumption.
		/// </summary>
		public SubscriptionType SubscriptionType { get; set; } = SubscriptionType.Shared;

		/// <summary>
		/// Gets or sets the maximum number of messages to process in a single batch. Must be positive.
		/// </summary>
		public int BatchSize { get; set; } = 100; // Default batch size

		/// <summary>
		/// Max total bytes in a batch
		/// </summary>
		public long MaxNumBytes { get; set; } = 1024 * 1024; // 1MB default

		/// <summary>
		/// Max wait time for a batch to be completed. Default is 100ms.
		/// </summary>
		public int TimeoutMs { get; set; } = 100;
	}
}

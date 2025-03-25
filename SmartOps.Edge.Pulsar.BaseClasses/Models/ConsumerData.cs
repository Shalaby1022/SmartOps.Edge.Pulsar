

namespace SmartOps.Edge.Pulsar.BaseClasses.Models
{
	public class ConsumerData
	{
		public string ServiceUrl { get; set; } = "pulsar://localhost:6650";
		public string SubscriptionName { get; set; } = "DefaultSubscription"; // Optional with default
		public string? Topic { get; set; } // Nullable, optional
	}
}

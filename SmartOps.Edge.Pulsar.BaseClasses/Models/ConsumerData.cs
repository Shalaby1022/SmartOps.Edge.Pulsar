

namespace SmartOps.Edge.Pulsar.BaseClasses.Models
{
	public class ConsumerData
	{
		#region Story Point 1: Accept topic name and subscription name
		public string ServiceUrl { get; set; } = "pulsar://localhost:6650";
		public string SubscriptionName { get; set; } = "DefaultSubscription"; // Optional with default
		public string? Topic { get; set; } // Nullable, optional
		#endregion
	}
}

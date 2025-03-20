namespace SmartOps.Edge.Pulsar.BaseClasses.Models;
using System.Text.RegularExpressions;
	public class CreateTopicData
	{
		private const int MIN_PARTITIONS = 1;
		private const int MIN_RETENTION_MINUTES = 1;
		private static readonly Regex ValidTopicNameRegex = new Regex("^[a-zA-Z0-9._-]+$", RegexOptions.Compiled);

		public string ServiceUrl { get; set; }
		public string Name { get; set; }
		public int NumPartitions { get; set; } = 1;
		public string RetentionMins { get; set; } = ConstantValues.DEFAULT_TOPIC_RETENTION_MINS; 
		public int? RetentionSizeMB { get; set; }
		public int? ReplicationFactor { get; set; } // TO BE Addressed in future.
		public string Tenant { get; set; } = "public"; // Default tenant
		public string Namespace { get; set; } = "default"; // Default namespace

		public CreateTopicData(string serviceUrl, string name)
		{
			if (string.IsNullOrWhiteSpace(serviceUrl))
				throw new ArgumentException("Service URL is required.", nameof(serviceUrl));

			if (string.IsNullOrWhiteSpace(name))
				throw new ArgumentNullException("Topic name is required.", nameof(name));
			if (!ValidTopicNameRegex.IsMatch(name))
				throw new ArgumentException("Invalid topic name format. Allowed: alphanumeric, dots, dashes, and underscores.", nameof(name));

			ServiceUrl = serviceUrl;
			Name = name;
		}
		
}


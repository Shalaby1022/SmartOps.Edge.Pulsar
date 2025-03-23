namespace SmartOps.Edge.Pulsar.BaseClasses.Models;
using System.Text.RegularExpressions;

	/// <summary>
	/// Represents the data required to create a Pulsar topic.
	/// </summary>
	public class CreateTopicData
	{
		/// <summary>
		/// Regular expression used to validate topic names.
		/// Allows only alphanumeric characters, dots (.), dashes (-), and underscores (_).
		/// </summary>
		private static readonly Regex ValidTopicNameRegex = new Regex("^[a-zA-Z0-9._-]+$", RegexOptions.Compiled);

		/// <summary>
		/// Gets or sets the Pulsar service URL.
		/// </summary>
		public string ServiceUrl { get; set; }

		/// <summary>
		/// Gets or sets the name of the topic to create.
		/// </summary>
		public string TopicName { get; set; }

		/// <summary>
		/// Gets or sets the number of partitions for the topic. Defaults to 1.
		/// </summary>
		public int NumPartitions { get; set; } = 1;

		/// <summary>
		/// Gets or sets the retention time in minutes for the topic. Defaults to a constant value.
		/// </summary>
		public string RetentionMins { get; set; } = ConstantValues.DEFAULT_TOPIC_RETENTION_MINS;

		/// <summary>
		/// Gets or sets the retention size in megabytes for the topic.
		/// </summary>
		public int? RetentionSizeMB { get; set; }

		/// <summary>
		/// Gets or sets the replication factor for the topic. Reserved for future use.
		/// </summary>
		public int? ReplicationFactor { get; set; }

		/// <summary>
		/// Gets or sets the tenant name. Defaults to "public".
		/// </summary>
		public string Tenant { get; set; } = "public";

		/// <summary>
		/// Gets or sets the namespace for the topic. Defaults to "default".
		/// </summary>
		public string Namespace { get; set; } = "default";

		/// <summary>
		/// Initializes a new instance of the <see cref="CreateTopicData"/> class.
		/// </summary>
		/// <param name="serviceUrl">The Pulsar service URL.</param>
		/// <param name="name">The name of the topic to create.</param>
		/// <exception cref="ArgumentNullException">
		/// Thrown if <paramref name="serviceUrl"/> or <paramref name="name"/> is null or empty,
		/// or if <paramref name="name"/> is in an invalid format.
		/// </exception>
		public CreateTopicData(string serviceUrl, string name)
		{
			if (string.IsNullOrWhiteSpace(serviceUrl))
				throw new ArgumentNullException(nameof(serviceUrl), "Service URL is required.");
			if (string.IsNullOrWhiteSpace(name))
				throw new ArgumentNullException(nameof(name), "Topic name is required.");
			if (!ValidTopicNameRegex.IsMatch(name))
				throw new ArgumentNullException(nameof(name), "Invalid topic name format. Allowed: alphanumeric, dots, dashes, and underscores.");

			ServiceUrl = serviceUrl;
			TopicName = name;
		}
	}


namespace SmartOps.Edge.Pulsar.BaseClasses.Models
{
	public static class ConstantValues
	{
		/// <summary>
		/// Default retention time in Minutes (7 days).
		/// </summary>
		public const string DEFAULT_TOPIC_RETENTION_MINS = "10080"; // 7 days in mins

		/// <summary>
		/// Default consuming waiting time in milliseconds for Pulsar consumers.
		/// </summary>
		public const int CONSUMING_WAITING_TIME_MILLI_SECOND = 100;

		/// <summary>
		/// Error code for topic creation failure in Pulsar.
		/// </summary>
		public const string TOPIC_CREATION_ERROR = "Err-PLS-1007";
		
		/// <summary>
		/// Error code for topic already existing due to a race condition.
		/// </summary>
		public const string TOPIC_ALREADY_EXISTS = "Err-PLS-1009";

		/// <summary>
		/// Error code for permission denied.
		/// </summary>
		public const string PERMISSION_DENIED = "Err-PLS-1010";

		/// <summary>
		/// Error code for subscription failure in Pulsar.
		/// </summary>
		public const string SUBSCRIBE_ERROR = "Err-PLS-1004";

		/// <summary>
		/// Error code for batch subscription failure in Pulsar.
		/// </summary>
		public const string SUBSCRIBE_BATCH_ERROR = "Err-PLS-1006";

		/// <summary>
		/// Error code for retrieving all topics failure in Pulsar.
		/// </summary>
		public const string RETRIEVE_ALL_TOPICS_ERROR = "Err-PLS-1008";
		/// <summary>
		/// General error code when a process is canceled or aborted.
		/// </summary>
		public const string REQUESTED_ABORTED = "Err-COM-0499";
		/// <summary>
		/// Error code for message publishing failure.
		/// </summary>
		public const string PRODUCE_MESSAGE_ERROR = "Err-PLS-1001";

	}
}

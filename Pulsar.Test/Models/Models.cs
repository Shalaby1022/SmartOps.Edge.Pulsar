namespace Pulsar.Test.Models
{
	public static class TestConstantValues

	{

    	public const string BROKER_URL = "pulsar://localhost:6650";  

		public static readonly string ADMIN_URL = BROKER_URL.Replace("pulsar://", "http://").Replace(":6650", ":8080");

		public const string EXISTING_TOPIC = "valid.topic-123";

		public const string NEW_TOPIC = "new-valid-topic";

		public static string NEW_DYNAMIC_TOPIC => $"public/default/{Guid.NewGuid()}"; 

		public static string NEW_TOPIC_WITH_RETENTION => $"public/default/{Guid.NewGuid()}"; 

		public const int DEFAULT_PARTITIONS = 1;

		public const int RETENTION_MINUTES = 10;
 
		public const string TEST_SUBSCRIPTION_NAME = "test-subscription";

		public const string TEST_TOPIC = "persistent/public/default/test-topic";

		public const uint DEFAULT_PREFETCH_COUNT = 1000;

	}
}

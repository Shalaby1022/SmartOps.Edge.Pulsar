using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using SmartOps.Edge.Pulsar.BaseClasses.Contracts;
using SmartOps.Edge.Pulsar.BaseClasses.Models;

namespace SmartOps.Edge.Pulsar.Messages.Manager
{
	/// <summary>
	/// Manages topics in Apache Pulsar, including creation and retrieval.
	/// </summary>
	public class TopicManager : ITopicManager
	{
		private readonly IHttpClientFactory _httpClientFactory;
		private readonly HttpClient _httpClient;
		private bool _disposed;

		public TopicManager(IHttpClientFactory httpClientFactory)
		{
			_httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
			_httpClient = _httpClientFactory.CreateClient();
		}

		#region Create Topic

		/// <summary>
		/// Creates a new topic in Pulsar.
		/// </summary>
		public async Task<BaseResponse> CreateTopic(CreateTopicData createTopicData)
		{
			if (createTopicData == null)
				throw new ArgumentNullException(nameof(createTopicData));

			if (createTopicData.NumPartitions < 0)
				throw new ArgumentException("Number of partitions cannot be negative.", nameof(createTopicData.NumPartitions));

			// Validate Retention Minutes before proceeding
			if (!string.IsNullOrEmpty(createTopicData.RetentionMins) &&
				int.TryParse(createTopicData.RetentionMins, out int retention) &&
				retention <= 0)
			{
				throw new ArgumentException("Retention minutes must be greater than 0.", nameof(createTopicData.RetentionMins));
			}

			BaseResponse response = new();
			string adminUrl = ConvertToAdminUrl(createTopicData.ServiceUrl);
			string topicPath = $"persistent/{createTopicData.Tenant}/{createTopicData.Namespace}/{createTopicData.TopicName}";
			string topicPartitionsUrl = $"{adminUrl}/admin/v2/{topicPath}/partitions";

			try
			{
				if (await TopicExists(topicPartitionsUrl))
				{
					response.IsSuccess = true;
					response.Message = $"Topic '{createTopicData.TopicName}' already exists.";
					return response;
				}

				await CreateTopicInPulsar(topicPartitionsUrl, createTopicData.NumPartitions);
				await ConfigureRetention(createTopicData, adminUrl);

				Console.WriteLine($"[INFO] Topic '{createTopicData.TopicName}' created successfully.");
				response.IsSuccess = true;
				response.Message = $"Topic '{createTopicData.TopicName}' created successfully.";
			}
			catch (HttpRequestException ex)
			{
				Console.WriteLine($"[ERROR] HTTP error: {ex.Message}");
				response.IsSuccess = false;
				response.Message = $"Failed to create topic: {ex.Message}";
				response.ErrorCode = ConstantValues.TOPIC_CREATION_ERROR;
			}
			catch (Exception ex)
			{
				Console.WriteLine($"[ERROR] Unexpected error: {ex.Message}");
				response.IsSuccess = false;
				response.Message = $"Failed to create topic: {ex.Message}";
				response.ErrorCode = ConstantValues.TOPIC_CREATION_ERROR;
			}

			return response;
		}
		/// <summary>
		/// Checks if a topic already exists in Pulsar.
		/// </summary>
		protected async Task<bool> TopicExists(string topicPartitionsUrl)
		{
			Console.WriteLine($"[DEBUG] Checking existence: GET {topicPartitionsUrl}");
			var checkResponse = await _httpClient.GetAsync(topicPartitionsUrl);

			return checkResponse.StatusCode == HttpStatusCode.OK;
		}

		/// <summary>
		/// Sends a request to create a topic.
		/// </summary>
		private async Task CreateTopicInPulsar(string topicPartitionsUrl, int numPartitions)
		{
			Console.WriteLine($"[DEBUG] Creating topic: PUT {topicPartitionsUrl} with {numPartitions} partitions");

			var requestContent = new StringContent(numPartitions.ToString(), Encoding.UTF8, "application/json");
			var createResponse = await _httpClient.PutAsync(topicPartitionsUrl, requestContent);

			if (!createResponse.IsSuccessStatusCode)
			{
				string errorContent = await createResponse.Content.ReadAsStringAsync();
				Console.WriteLine($"[ERROR] Creation failed: {createResponse.StatusCode} - {errorContent}");
				throw new HttpRequestException($"Failed to create topic: {createResponse.StatusCode} - {errorContent}");
			}
		}

		/// <summary>
		/// Configures topic retention settings in Pulsar.
		/// </summary>
		private async Task ConfigureRetention(CreateTopicData createTopicData, string adminUrl)
		{
			if (createTopicData.RetentionMins == ConstantValues.DEFAULT_TOPIC_RETENTION_MINS || string.IsNullOrEmpty(createTopicData.RetentionMins))
				return;

			var retentionTimeInMinutes = int.Parse(createTopicData.RetentionMins);
			var retentionConfig = new { retentionTimeInMinutes, retentionSizeInMB = createTopicData.RetentionSizeMB ?? -1 };
			var retentionContent = new StringContent(JsonSerializer.Serialize(retentionConfig), Encoding.UTF8, "application/json");
			var retentionUrl = $"{adminUrl}/admin/v2/namespaces/{createTopicData.Tenant}/{createTopicData.Namespace}/retention";

			Console.WriteLine($"[DEBUG] Setting retention: POST {retentionUrl}");
			var retentionResponse = await _httpClient.PostAsync(retentionUrl, retentionContent);

			if (!retentionResponse.IsSuccessStatusCode)
			{
				string errorContent = await retentionResponse.Content.ReadAsStringAsync();
				Console.WriteLine($"[ERROR] Retention failed: {retentionResponse.StatusCode} - {errorContent}");
				throw new HttpRequestException($"Failed to set retention: {retentionResponse.StatusCode} - {errorContent}");
			}
		}

		#endregion Create Topic

		#region Get Topics

		/// <summary>
		/// Retrieves a list of topics in the Pulsar cluster.
		/// </summary>
		public async Task<List<string>> GetTopics(string serviceUrl)
		{
			string adminUrl = ConvertToAdminUrl(serviceUrl);
			string topicsUrl = $"{adminUrl}/admin/v2/persistent/public/default";

			try
			{
				Console.WriteLine($"[DEBUG] Fetching topics: GET {topicsUrl}");
				var response = await _httpClient.GetAsync(topicsUrl);

				if (!response.IsSuccessStatusCode)
					throw new HttpRequestException($"Failed to fetch topics: {response.StatusCode}");

				var content = await response.Content.ReadAsStringAsync();
				Console.WriteLine($"[DEBUG] Raw API Response: {content}");

				if (string.IsNullOrWhiteSpace(content))
				{
					Console.WriteLine("[ERROR] Empty response body!");
					return new List<string>();
				}

				return JsonSerializer.Deserialize<List<string>>(content, new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ?? new List<string>();
			}
			catch (Exception ex)
			{
				Console.WriteLine($"[ERROR] Failed to retrieve topics: {ex.Message}");
				return new List<string>();
			}
		}

		#endregion Get Topics

		#region Utility Methods
		/// <summary>
		/// Converts Pulsar's broker URL to the corresponding admin REST API URL.
		/// </summary>
		protected static string ConvertToAdminUrl(string serviceUrl)
		{
			return serviceUrl.Replace("pulsar://", "http://").Replace(":6650", ":8081");
		}
		#endregion Utility Methods

		#region Disposal
		public void Dispose()
		{
		}
		#endregion Disposal
	}
}

namespace Pulsar.Test.UnitTest
{
	public class TopicManagerTests
	{
		private readonly TopicManager _business;
		private readonly string _topicName;
		private readonly IHttpClientFactory _mockHttpClientFactory;
		private readonly HttpClient _mockHttpClient;

		public TopicManagerTests()
		{
			_mockHttpClientFactory = Substitute.For<IHttpClientFactory>();
			var mockHandler = new MockHttpMessageHandler((request, cancellationToken) =>
			{
				var expectedTopics = new List<string> { "topic1", "topic2", "valid.topic-123" };
				var jsonContent = JsonSerializer.Serialize(expectedTopics ?? new List<string>());
				Console.WriteLine($"[DEBUG] Mock Response JSON (Setup): {jsonContent}");
				return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
				{
					Content = new StringContent(jsonContent, Encoding.UTF8, "application/json")
				});
			});
			_mockHttpClient = new HttpClient(mockHandler);
			_mockHttpClientFactory.CreateClient(Arg.Any<string>()).Returns(_mockHttpClient);
			_business = new TopicManager(_mockHttpClientFactory);
			_topicName = TestConstantValues.EXISTING_TOPIC;
		}

		private TopicManager CreateTopicManagerWithMock(HttpStatusCode statusCode, HttpMethod method, string errorContent = null)
		{
			var mockHandler = new MockHttpMessageHandler((request, cancellationToken) =>
			{
				Console.WriteLine($"[DEBUG] Request: {request.Method} {request.RequestUri}");
				if (request.Method == method)
				{
					return Task.FromResult(new HttpResponseMessage(statusCode)
					{
						Content = statusCode == HttpStatusCode.OK
							? null
							: new StringContent(errorContent ?? "Error", Encoding.UTF8, "application/json")
					});
				}
				return Task.FromResult(new HttpResponseMessage(HttpStatusCode.BadRequest));
			});
			var httpClient = new HttpClient(mockHandler);
			var mockHttpClientFactory = Substitute.For<IHttpClientFactory>();
			mockHttpClientFactory.CreateClient(Arg.Any<string>()).Returns(httpClient);
			return new TopicManager(mockHttpClientFactory);
		}

		#region Topic Creation Tests

		[Fact]
		[Description("Check if creating a valid topic does not throw an exception.")]
		public async Task CreateTopic_ValidInput_DoesNotThrowException()
		{
			var topicData = new CreateTopicData(TestConstantValues.BROKER_URL, _topicName);
			var exception = await Record.ExceptionAsync(() => _business.CreateTopic(topicData));
			Assert.Null(exception);
		}

		[Fact]
		[Description("Check if creating an existing topic returns success without creation.")]
		public async Task CreateTopic_TopicExists_ReturnsSuccessWithoutCreation()
		{
			var topicData = new CreateTopicData(TestConstantValues.BROKER_URL, _topicName);
			var response = await _business.CreateTopic(topicData);
			Assert.True(response.IsSuccess);
			Assert.Equal($"Topic '{_topicName}' already exists.", response.Message);
		}

		[Fact]
		[Description("Check if creating a topic with empty name throws an exception.")]
		public void CreateTopic_EmptyName_ThrowsException()
		{
			var exception = Assert.Throws<ArgumentNullException>(() => new CreateTopicData(TestConstantValues.BROKER_URL, ""));
			Assert.Contains("Topic name is required", exception.Message);
		}

		[Fact]
		[Description("Check if creating a topic with negative partitions throws an exception.")]
		public async Task CreateTopic_NegativePartitions_ThrowsArgumentException()
		{
			var topicData = new CreateTopicData(TestConstantValues.BROKER_URL, TestConstantValues.NEW_TOPIC) { NumPartitions = -1 };
			var exception = await Assert.ThrowsAsync<ArgumentException>(() => _business.CreateTopic(topicData));
			Assert.Contains("Number of partitions cannot be negative.", exception.Message);
		}

		#endregion

		#region Topic Fetching

		[Fact]
		[Description("Check if fetching topics returns a non-empty list.")]
		public async Task GetTopics_ValidRequest_ReturnsListOfTopics()
		{
			var expectedTopics = new List<string> { "topic1", "topic2", _topicName };
			var jsonContent = JsonSerializer.Serialize(expectedTopics ?? new List<string>());
			var mockHandler = new MockHttpMessageHandler((request, cancellationToken) =>
			{
				return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
				{
					Content = new StringContent(jsonContent, Encoding.UTF8, "application/json")
				});
			});
			var testHttpClient = new HttpClient(mockHandler);
			_mockHttpClientFactory.CreateClient(Arg.Any<string>()).Returns(testHttpClient);
			var topics = await _business.GetTopics(TestConstantValues.BROKER_URL);
			Assert.NotNull(topics);
			Assert.NotEmpty(topics);
			Assert.Contains(_topicName, topics);
		}

		#endregion

		#region Retention Minutes Tests

		[Fact]
		[Description("Check if creating a topic with retention minutes set to 0 throws an exception.")]
		public async Task CreateTopic_ZeroRetentionMinutes_ThrowsException()
		{
			var topicData = new CreateTopicData(TestConstantValues.BROKER_URL, TestConstantValues.NEW_TOPIC) { RetentionMins = "0" };
			var exception = await Assert.ThrowsAsync<ArgumentException>(() => _business.CreateTopic(topicData));
			Assert.Contains("Retention minutes must be greater than 0", exception.Message);
		}

		[Fact]
		[Description("Check if creating a topic with negative retention minutes throws an exception.")]
		public async Task CreateTopic_NegativeRetentionMinutes_ThrowsException()
		{
			var topicData = new CreateTopicData(TestConstantValues.BROKER_URL, TestConstantValues.NEW_TOPIC) { RetentionMins = "-5" };
			var exception = await Assert.ThrowsAsync<ArgumentException>(() => _business.CreateTopic(topicData));
			Assert.Contains("Retention minutes must be greater than 0", exception.Message);
		}

		[Fact]
		[Description("Check if creating a topic with valid retention minutes does not throw an exception.")]
		public async Task CreateTopic_ValidRetentionMinutes_DoesNotThrowException()
		{
			var topicData = new CreateTopicData(TestConstantValues.BROKER_URL, TestConstantValues.NEW_TOPIC) { RetentionMins = "5" };
			var exception = await Record.ExceptionAsync(() => _business.CreateTopic(topicData));
			Assert.Null(exception);
		}

		#endregion

		#region CreateTopicInPulsar Tests (Using Reflection)

		[Fact]
		[Description("Check if CreateTopicInPulsar succeeds for a non-existent topic via reflection.")]
		public async Task CreateTopicInPulsar_NonExistentTopic_Succeeds()
		{
			var topicData = new CreateTopicData(TestConstantValues.BROKER_URL, TestConstantValues.NEW_TOPIC) { NumPartitions = 1 };
			var topicManager = CreateTopicManagerWithMock(HttpStatusCode.OK, HttpMethod.Put);
			string adminUrl = ReflectionTestHelpers.InvokeProtectedMethod<string>(topicManager, "ConvertToAdminUrl", TestConstantValues.BROKER_URL);
			string topicPath = $"persistent/{topicData.Tenant}/{topicData.Namespace}/{topicData.TopicName}";
			string topicPartitionsUrl = $"{adminUrl}/admin/v2/{topicPath}/partitions";

			await ReflectionTestHelpers.InvokePrivateMethodAsync(topicManager, "CreateTopicInPulsar", topicPartitionsUrl, topicData.NumPartitions);

			Assert.True(true);
		}

		[Fact]
		[Description("Check if CreateTopicInPulsar throws an exception on failure via reflection.")]
		public async Task CreateTopicInPulsar_Failure_ThrowsHttpRequestException()
		{
			// Arrange
			var topicData = new CreateTopicData(TestConstantValues.BROKER_URL, TestConstantValues.NEW_TOPIC) { NumPartitions = 1 };
			var topicManager = CreateTopicManagerWithMock(HttpStatusCode.BadRequest, HttpMethod.Put, "Creation failed");
			string adminUrl = ReflectionTestHelpers.InvokeProtectedMethod<string>(topicManager, "ConvertToAdminUrl", TestConstantValues.BROKER_URL);
			string topicPath = $"persistent/{topicData.Tenant}/{topicData.Namespace}/{topicData.TopicName}";
			string topicPartitionsUrl = $"{adminUrl}/admin/v2/{topicPath}/partitions";

			// Act
			var task = ReflectionTestHelpers.InvokePrivateMethodAsync(topicManager, "CreateTopicInPulsar", topicPartitionsUrl, topicData.NumPartitions);

			// Assert
			var exception = await Assert.ThrowsAsync<HttpRequestException>(() => task);
			Assert.Contains("Failed to create topic: BadRequest - Creation failed", exception.Message);
		}

		[Fact]
		[Description("Check if ConfigureRetention succeeds with valid retention via reflection.")]
		public async Task ConfigureRetention_ValidRetention_Succeeds()
		{
			var topicData = new CreateTopicData(TestConstantValues.BROKER_URL, TestConstantValues.NEW_TOPIC)
			{
				NumPartitions = 1,
				RetentionMins = "10"
			};
			var topicManager = CreateTopicManagerWithMock(HttpStatusCode.OK, HttpMethod.Post);
			string adminUrl = ReflectionTestHelpers.InvokeProtectedMethod<string>(topicManager, "ConvertToAdminUrl", TestConstantValues.BROKER_URL);

			await ReflectionTestHelpers.InvokePrivateMethodAsync(topicManager, "ConfigureRetention", topicData, adminUrl);

			Assert.True(true);
		}

		[Fact]
		[Description("Check if ConfigureRetention throws an exception on failure via reflection.")]
		public async Task ConfigureRetention_Failure_ThrowsHttpRequestException()
		{
			// Arrange
			var topicData = new CreateTopicData(TestConstantValues.BROKER_URL, TestConstantValues.NEW_TOPIC)
			{
				NumPartitions = 1,
				RetentionMins = "10"
			};
			var topicManager = CreateTopicManagerWithMock(HttpStatusCode.BadRequest, HttpMethod.Post, "Retention failed");
			string adminUrl = ReflectionTestHelpers.InvokeProtectedMethod<string>(topicManager, "ConvertToAdminUrl", TestConstantValues.BROKER_URL);

			// Act
			var task = ReflectionTestHelpers.InvokePrivateMethodAsync(topicManager, "ConfigureRetention", topicData, adminUrl);

			// Assert
			var exception = await Assert.ThrowsAsync<HttpRequestException>(() => task);
			Assert.Contains("Failed to set retention: BadRequest - Retention failed", exception.Message);
		}

		#endregion

		#region CreateTopic Success and Exception Tests

		[Fact]
		[Description("Check if CreateTopic succeeds when topic is created and retention is configured.")]
		public async Task CreateTopic_NonExistentTopicWithRetention_Succeeds()
		{
			var topicData = new CreateTopicData(TestConstantValues.BROKER_URL, TestConstantValues.NEW_TOPIC)
			{
				NumPartitions = 1,
				RetentionMins = "10"
			};
			var mockHandler = new MockHttpMessageHandler((request, cancellationToken) =>
			{
				Console.WriteLine($"[DEBUG] Request: {request.Method} {request.RequestUri}");
				if (request.Method == HttpMethod.Get) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound));
				if (request.Method == HttpMethod.Put) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
				if (request.Method == HttpMethod.Post) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
				return Task.FromResult(new HttpResponseMessage(HttpStatusCode.BadRequest));
			});
			var httpClient = new HttpClient(mockHandler);
			var mockHttpClientFactory = Substitute.For<IHttpClientFactory>();
			mockHttpClientFactory.CreateClient(Arg.Any<string>()).Returns(httpClient);
			var topicManager = new TopicManager(mockHttpClientFactory);
			var response = await topicManager.CreateTopic(topicData);
			Assert.True(response.IsSuccess);
			Assert.Equal($"Topic '{TestConstantValues.NEW_TOPIC}' created successfully.", response.Message);
		}

		[Fact]
		[Description("Check if CreateTopic handles HttpRequestException from CreateTopicInPulsar.")]
		public async Task CreateTopic_HttpRequestException_HandlesCatchBlock()
		{
			var topicData = new CreateTopicData(TestConstantValues.BROKER_URL, TestConstantValues.NEW_TOPIC) { NumPartitions = 1 };
			var mockHandler = new MockHttpMessageHandler((request, cancellationToken) =>
			{
				Console.WriteLine($"[DEBUG] Request: {request.Method} {request.RequestUri}");
				if (request.Method == HttpMethod.Get) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound));
				if (request.Method == HttpMethod.Put) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.BadRequest)
				{
					Content = new StringContent("Creation failed", Encoding.UTF8, "application/json")
				});
				return Task.FromResult(new HttpResponseMessage(HttpStatusCode.BadRequest));
			});
			var httpClient = new HttpClient(mockHandler);
			var mockHttpClientFactory = Substitute.For<IHttpClientFactory>();
			mockHttpClientFactory.CreateClient(Arg.Any<string>()).Returns(httpClient);
			var topicManager = new TopicManager(mockHttpClientFactory);
			var response = await topicManager.CreateTopic(topicData);
			Assert.False(response.IsSuccess);
			Assert.Contains("Failed to create topic: BadRequest - Creation failed", response.Message);
			Assert.Equal(ConstantValues.TOPIC_CREATION_ERROR, response.ErrorCode);
		}

		[Fact]
		[Description("Check if CreateTopic handles general Exception from ConfigureRetention.")]
		public async Task CreateTopic_GeneralException_HandlesCatchBlock()
		{
			var topicData = new CreateTopicData(TestConstantValues.BROKER_URL, TestConstantValues.NEW_TOPIC)
			{
				NumPartitions = 1,
				RetentionMins = "invalid"
			};
			var mockHandler = new MockHttpMessageHandler((request, cancellationToken) =>
			{
				Console.WriteLine($"[DEBUG] Request: {request.Method} {request.RequestUri}");
				if (request.Method == HttpMethod.Get) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound));
				if (request.Method == HttpMethod.Put) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
				return Task.FromResult(new HttpResponseMessage(HttpStatusCode.BadRequest));
			});
			var httpClient = new HttpClient(mockHandler);
			var mockHttpClientFactory = Substitute.For<IHttpClientFactory>();
			mockHttpClientFactory.CreateClient(Arg.Any<string>()).Returns(httpClient);
			var topicManager = new TopicManager(mockHttpClientFactory);
			var response = await topicManager.CreateTopic(topicData);
			Assert.False(response.IsSuccess);
			Assert.Contains("Failed to create topic:", response.Message);
			Assert.Equal(ConstantValues.TOPIC_CREATION_ERROR, response.ErrorCode);
		}

		#endregion
	}
}
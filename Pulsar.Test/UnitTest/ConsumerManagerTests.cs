using DotPulsar.Abstractions;
using SmartOps.Edge.Pulsar.Bus.Messages.Manager;
using System.Buffers;

namespace Pulsar.Test.UnitTest
{
	public class ConsumerManagerTests : IDisposable
	{
		private readonly IPulsarClient _mockPulsarClient;
		private readonly IConsumer<byte[]> _mockConsumer;
		private readonly string _topic = Models.TestConstantValues.TEST_TOPIC;
		private T GetPrivateField<T>(object instance, string fieldName) => ReflectionTestHelpers.GetPrivateField<T>(instance, fieldName);
		private void SetPrivateField<T>(object instance, string fieldName, T value) => ReflectionTestHelpers.SetPrivateField(instance, fieldName, value);

		public ConsumerManagerTests()
		{
			_mockPulsarClient = Substitute.For<IPulsarClient>();
			_mockConsumer = Substitute.For<IConsumer<byte[]>>();
		}
		public void Dispose()
		{
			_mockPulsarClient.ClearReceivedCalls();
			_mockConsumer.ClearReceivedCalls();
		}
		private ConsumerData CreateValidConsumerData() => new()
		{
			ServiceUrl = Models.TestConstantValues.BROKER_URL,
			SubscriptionName = Models.TestConstantValues.TEST_SUBSCRIPTION_NAME
		};

		[Fact]
		[Description("Throws ArgumentException when constructed with invalid prefetch count")]
		public void Constructor_InvalidPrefetchCount_ThrowsArgumentException()
		{
			// Act & Assert
			var exception = Assert.Throws<ArgumentException>(() => new ConsumerManager(messagePrefetchCount: 0));
			Assert.Contains("Message prefetch count must be positive.", exception.Message);
		}

		[Fact]
		[Description("Successfully constructs ConsumerManager with valid parameters")]
		public void Constructor_ValidParameters_DoesNotThrow()
		{
			// Act
			var manager = new ConsumerManager(messagePrefetchCount: 1000);

			// Assert
			Assert.NotNull(manager);
		}

		[Fact]
		[Description("Successfully connects a consumer with valid ConsumerData")]
		public void ConnectConsumer_ValidData_Succeeds()
		{
			// Arrange
			var manager = new ConsumerManager(_mockPulsarClient);
			var consumerData = CreateValidConsumerData();
			Action<IConsumer<byte[]>, Exception> errorHandler = (_, _) => { };

			// Act
			manager.ConnectConsumer(consumerData, errorHandler);

			// Assert
			Assert.NotNull(manager);
			Assert.Equal(Models.TestConstantValues.TEST_SUBSCRIPTION_NAME, GetPrivateField<string>(manager, "_subscriptionName"));
		}

		[Fact]
		[Description("Sets up Pulsar client and assigns subscription name if client is not pre-initialized")]
		public void ConnectConsumer_InitializesClientAndSubscriptionName()
		{
			// Arrange
			var manager = new ConsumerManager(); // no injected client
			var consumerData = new ConsumerData
			{
				ServiceUrl = Models.TestConstantValues.BROKER_URL,
				SubscriptionName = Models.TestConstantValues.TEST_SUBSCRIPTION_NAME
			};
			Action<IConsumer<byte[]>, Exception> errorHandler = (_, _) => { };

			// Act
			manager.ConnectConsumer(consumerData, errorHandler);

			// Assert
			var resultClient = GetPrivateField<IPulsarClient>(manager, "_client");
			var resultSubName = GetPrivateField<string>(manager, "_subscriptionName");

			Assert.NotNull(resultClient);
			Assert.Equal(consumerData.SubscriptionName, resultSubName);
		}

		[Fact]
		[Description("Throws ArgumentNullException when ConsumerData is null")]
		public void ConnectConsumer_NullData_ThrowsArgumentNullException()
		{
			// Arrange
			var manager = new ConsumerManager(_mockPulsarClient);
			Action<IConsumer<byte[]>, Exception> errorHandler = (_, _) => { };

			// Act & Assert
			var exception = Assert.Throws<ArgumentNullException>(() => manager.ConnectConsumer(null, errorHandler));
			Assert.Contains("consumerData", exception.Message);
		}

		[Fact]
		[Description("Throws ArgumentException when SubscriptionName is empty")]
		public void ConnectConsumer_EmptySubscriptionName_ThrowsArgumentException()
		{
			// Arrange
			var manager = new ConsumerManager(_mockPulsarClient);
			var consumerData = new ConsumerData { ServiceUrl = Models.TestConstantValues.BROKER_URL, SubscriptionName = "" };
			Action<IConsumer<byte[]>, Exception> errorHandler = (_, _) => { };

			// Act & Assert
			var exception = Assert.Throws<ArgumentException>(() => manager.ConnectConsumer(consumerData, errorHandler));
			Assert.Contains("SubscriptionName is required", exception.Message);
		}

		[Fact]
		[Description("Creates and sets up client if not pre-set during consumer connection")]
		public void ConnectConsumer_NoClient_SetsClientViaBuilder()
		{
			// Arrange
			var manager = new ConsumerManager();
			var consumerData = CreateValidConsumerData();
			Action<IConsumer<byte[]>, Exception> errorHandler = (_, _) => { };

			// Act
			manager.ConnectConsumer(consumerData, errorHandler);

			// Assert
			var client = GetPrivateField<IPulsarClient>(manager, "_client");
			Assert.NotNull(client);
			Assert.Equal(Models.TestConstantValues.TEST_SUBSCRIPTION_NAME, GetPrivateField<string>(manager, "_subscriptionName"));
		}

		[Fact]
		[Description("Throws UriFormatException for invalid service URL")]
		public void ConnectConsumer_InvalidServiceUrl_ThrowsException()
		{
			// Arrange
			var manager = new ConsumerManager();
			var consumerData = new ConsumerData { ServiceUrl = "invalid-url", SubscriptionName = "test-sub" };
			Action<IConsumer<byte[]>, Exception> errorHandler = (_, _) => { };

			// Act & Assert
			var exception = Assert.Throws<UriFormatException>(() =>
			{
				try
				{
					manager.ConnectConsumer(consumerData, errorHandler);
				}
				catch (UriFormatException ex)
				{
					Console.WriteLine($"[ERROR] Failed to connect client: {ex.Message}");
					throw;
				}
			});

			Assert.Equal("Invalid URI: The format of the URI could not be determined.", exception.Message);
		}

		[Fact]
		[Description("Throws InvalidOperationException when client is not connected for SubscribeAsync")]
		public async Task SubscribeAsync_ClientNotConnected_ThrowsInvalidOperationException()
		{
			// Arrange
			var manager = new ConsumerManager();
			var callback = Substitute.For<Func<SubscribeMessage<string>, Task>>();

			// Act & Assert
			var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
				manager.SubscribeAsync(_topic, callback, CancellationToken.None));
			Assert.Equal("Client not connected.", exception.Message);
		}

		[Fact]
		[Description("Throws ArgumentException when topic is empty in SubscribeAsync")]
		public async Task SubscribeAsync_EmptyTopic_ThrowsArgumentException()
		{
			// Arrange
			var manager = new ConsumerManager(_mockPulsarClient);
			var consumerData = CreateValidConsumerData();
			Action<IConsumer<byte[]>, Exception> errorHandler = (_, _) => { };
			manager.ConnectConsumer(consumerData, errorHandler);
			var callback = Substitute.For<Func<SubscribeMessage<string>, Task>>();

			// Act & Assert
			var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
				manager.SubscribeAsync("", callback, CancellationToken.None));
			Assert.Contains("Topic is required", exception.Message);
		}

		// 6/4 
		[Fact]
		[Description("Successfully subscribes to a topic and consumes a message")]
		public async Task SubscribeAsync_ValidSetup_ConsumesMessageAndInvokesCallback()
		{
			// Arrange
			var mockPulsarClient = Substitute.For<IPulsarClient>();
			var mockConsumer = Substitute.For<IConsumer<byte[]>>();

			var mockMessage = Substitute.For<IMessage<byte[]>>();
			var messageBytes = Encoding.UTF8.GetBytes("Test message");
			mockMessage.Data.Returns(new ReadOnlySequence<byte>(messageBytes));
			mockMessage.MessageId.Returns(new MessageId(1, 1, -1, 0, null));
			mockMessage.PublishTime.Returns((ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

			// Mock Receive to return one message, then complete
			var callCount = 0;
			mockConsumer.Receive(Arg.Any<CancellationToken>()).Returns(
				callInfo =>
				{
					callCount++;
					if (callCount == 1)
						return ValueTask.FromResult(mockMessage);
					throw new OperationCanceledException("Cancelled after one message for test");
				});
			mockConsumer.Topic.Returns(_topic);

			// Create ConsumerManager with mock client
			var manager = new ConsumerManager(mockPulsarClient);

			// Manually set private fields to simulate a connected state
			SetPrivateField(manager, "_client", mockPulsarClient);
			SetPrivateField(manager, "_consumer", mockConsumer);
			SetPrivateField(manager, "_subscriptionName", Models.TestConstantValues.TEST_SUBSCRIPTION_NAME);

			var receivedMessage = new List<SubscribeMessage<string>>();
			async Task Callback(SubscribeMessage<string> msg)
			{
				receivedMessage.Add(msg);
				await Task.CompletedTask;
			}

			using var cts = new CancellationTokenSource();

			// Act
			var subscribeTask = manager.SubscribeAsync(_topic, async msg =>
			{
				await Callback(msg);
				cts.Cancel(); // Cancel after processing one message
			}, cts.Token);

			await subscribeTask; // Wait for the task to complete

			// Assert
			Assert.Single(receivedMessage);
			Assert.Equal("Test message", receivedMessage[0].Data);
			Assert.Equal(_topic, receivedMessage[0].Topic);
			Assert.Equal(mockMessage.MessageId, receivedMessage[0].MessageId);
		}

		[Fact]
		[Description("Handles exception during message consumption and invokes callback with error")]
		public async Task SubscribeAsync_ReceiveThrowsException_InvokesCallbackWithError()
		{
			// Arrange
			var mockPulsarClient = Substitute.For<IPulsarClient>();
			var mockConsumer = Substitute.For<IConsumer<byte[]>>();

			mockConsumer.Receive(Arg.Any<CancellationToken>()).Returns(
				_ => new ValueTask<IMessage<byte[]>>(Task.FromException<IMessage<byte[]>>(new Exception("Broker connection failed")))
			);
			mockConsumer.Topic.Returns(_topic);

			var manager = new ConsumerManager(mockPulsarClient);

			SetPrivateField(manager, "_client", mockPulsarClient);
			SetPrivateField(manager, "_consumer", mockConsumer);
			SetPrivateField(manager, "_subscriptionName", Models.TestConstantValues.TEST_SUBSCRIPTION_NAME);

			var receivedMessage = new List<SubscribeMessage<string>>();
			async Task Callback(SubscribeMessage<string> msg)
			{
				receivedMessage.Add(msg);
				await Task.CompletedTask;
			}

			using var cts = new CancellationTokenSource();

			// Act
			await manager.SubscribeAsync(_topic, Callback, cts.Token);

			// Assert
			Assert.Single(receivedMessage);
			Assert.Null(receivedMessage[0].Data); // No data for error case
			Assert.Equal("Broker connection failed", receivedMessage[0].ExceptionMessage);
			Assert.Equal("SUBSCRIBE_ERROR", receivedMessage[0].ErrorCode);
		}

		[Fact]
		[Description("Successfully acknowledges a list of message IDs")]
		public async Task AcknowledgeAsync_ValidSetup_ReturnsSuccessResponse()
		{
			// Arrange
			var mockPulsarClient = Substitute.For<IPulsarClient>();
			var mockConsumer = Substitute.For<IConsumer<byte[]>>();

			// Mock Acknowledge to succeed
			mockConsumer.Acknowledge(Arg.Any<List<MessageId>>(), Arg.Any<CancellationToken>())
				.Returns(ValueTask.CompletedTask);

			var manager = new ConsumerManager(mockPulsarClient);
			SetPrivateField(manager, "_client", mockPulsarClient);
			SetPrivateField(manager, "_consumer", mockConsumer);

			var messageIds = new List<MessageId>
			{
				new MessageId(1, 1, -1, 0, null),
				new MessageId(2, 2, -1, 0, null)
			};

			// Act
			var response = await manager.AcknowledgeAsync(messageIds);

			// Assert
			Assert.True(response.IsSuccess);
			Assert.Equal("Messages acknowledged successfully", response.Message);
			Assert.Equal("", response.ErrorCode); // Updated: Expect empty string instead of null
			await mockConsumer.Received(1).Acknowledge(
				Arg.Is<List<MessageId>>(ids => ids.Count == 2 && ids[0].Equals(messageIds[0]) && ids[1].Equals(messageIds[1])),
				Arg.Any<CancellationToken>());
		}

		[Fact]
		[Description("Successfully acknowledges messages cumulatively up to a message ID")]
		public async Task AcknowledgeCumulativeAsync_ValidSetup_ReturnsSuccessResponse()
		{
			// Arrange
			var mockPulsarClient = Substitute.For<IPulsarClient>();
			var mockConsumer = Substitute.For<IConsumer<byte[]>>();

			// Mock AcknowledgeCumulative to succeed
			mockConsumer.AcknowledgeCumulative(Arg.Any<MessageId>(), Arg.Any<CancellationToken>())
				.Returns(ValueTask.CompletedTask);

			var manager = new ConsumerManager(mockPulsarClient);
			SetPrivateField(manager, "_client", mockPulsarClient);
			SetPrivateField(manager, "_consumer", mockConsumer);
			SetPrivateField(manager, "_subscriptionType", SubscriptionType.Exclusive); // Non-Shared subscription

			var messageId = new MessageId(3, 3, -1, 0, null);

			// Act
			var response = await manager.AcknowledgeCumulativeAsync(messageId);

			// Assert
			Assert.True(response.IsSuccess);
			Assert.Equal("Messages acknowledged cumulatively", response.Message);
			Assert.Equal("", response.ErrorCode); // Expect empty string based on previous test
			await mockConsumer.Received(1).AcknowledgeCumulative(
				Arg.Is<MessageId>(id => id.Equals(messageId)),
				Arg.Any<CancellationToken>());
		}

		[Fact]
		[Description("Successfully requests redelivery of unacknowledged messages")]
		public async Task RedeliverUnacknowledgedMessagesAsync_ValidSetup_ReturnsSuccessResponse()
		{
			// Arrange
			var mockPulsarClient = Substitute.For<IPulsarClient>();
			var mockConsumer = Substitute.For<IConsumer<byte[]>>();

			// Mock RedeliverUnacknowledgedMessages to succeed
			mockConsumer.RedeliverUnacknowledgedMessages(Arg.Any<IEnumerable<MessageId>>(), Arg.Any<CancellationToken>())
				.Returns(ValueTask.CompletedTask);

			var manager = new ConsumerManager(mockPulsarClient);
			SetPrivateField(manager, "_client", mockPulsarClient);
			SetPrivateField(manager, "_consumer", mockConsumer);

			var messageIds = new List<MessageId>
			{
				new MessageId(1, 1, -1, 0, null),
				new MessageId(2, 2, -1, 0, null)
			};

			// Act
			var response = await manager.RedeliverUnacknowledgedMessagesAsync(messageIds);

			// Assert
			Assert.True(response.IsSuccess);
			Assert.Equal("Requested redelivery of unacknowledged messages", response.Message);
			Assert.Equal("", response.ErrorCode); // Expect empty string based on BaseResponse default
			await mockConsumer.Received(1).RedeliverUnacknowledgedMessages(
				Arg.Is<IEnumerable<MessageId>>(ids => ids.Count() == 2 && ids.SequenceEqual(messageIds)),
				Arg.Any<CancellationToken>());
		}

		[Fact]
		[Description("Successfully requests redelivery of all unacknowledged messages")]
		public async Task RedeliverAllUnacknowledgedMessagesAsync_ValidSetup_ReturnsSuccessResponse()
		{
			// Arrange
			var mockPulsarClient = Substitute.For<IPulsarClient>();
			var mockConsumer = Substitute.For<IConsumer<byte[]>>();

			// Mock RedeliverUnacknowledgedMessages (no message IDs) to succeed
			mockConsumer.RedeliverUnacknowledgedMessages(Arg.Any<CancellationToken>())
				.Returns(ValueTask.CompletedTask);

			var manager = new ConsumerManager(mockPulsarClient);
			SetPrivateField(manager, "_client", mockPulsarClient);
			SetPrivateField(manager, "_consumer", mockConsumer);

			// Act
			var response = await manager.RedeliverAllUnacknowledgedMessagesAsync();

			// Assert
			Assert.True(response.IsSuccess);
			Assert.Equal("Requested redelivery of all unacknowledged messages", response.Message);
			Assert.Equal("", response.ErrorCode); // Expect empty string based on BaseResponse default
			await mockConsumer.Received(1).RedeliverUnacknowledgedMessages(Arg.Any<CancellationToken>());
		}

		[Fact]
		[Description("Throws ArgumentNullException when consumerData is null")]
		public async Task ProcessBatchAsync_NullConsumerData_ThrowsArgumentNullException()
		{
			// Arrange
			var manager = new ConsumerManager(_mockPulsarClient);

			// Act & Assert
			var exception = await Assert.ThrowsAsync<ArgumentNullException>(() =>
				manager.ProcessBatchAsync(null));
			Assert.Contains("consumerData", exception.Message);
		}

		[Fact]
		[Description("Throws ArgumentException when ServiceUrl is empty")]
		public async Task ProcessBatchAsync_EmptyServiceUrl_ThrowsArgumentException()
		{
			// Arrange
			var manager = new ConsumerManager(_mockPulsarClient);
			var consumerData = new ConsumerData
			{
				ServiceUrl = "",
				SubscriptionName = "test-sub",
				TopicName = "test-topic",
				BatchSize = 10,
				MaxNumBytes = 1000,
				TimeoutMs = 5000
			};

			// Act & Assert
			var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
				manager.ProcessBatchAsync(consumerData));
			Assert.Contains("Service URL is required", exception.Message);
		}
		[Fact]
		[Description("Throws ArgumentException when SubscriptionName is empty")]
		public async Task ProcessBatchAsync_EmptySubscriptionName_ThrowsArgumentException()
		{
			// Arrange
			var manager = new ConsumerManager(_mockPulsarClient);
			var consumerData = new ConsumerData
			{
				ServiceUrl = Models.TestConstantValues.BROKER_URL,
				SubscriptionName = "",
				TopicName = "test-topic",
				BatchSize = 10,
				MaxNumBytes = 1000,
				TimeoutMs = 5000
			};

			// Act & Assert
			var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
				manager.ProcessBatchAsync(consumerData));
			Assert.Contains("Subscription name is required", exception.Message);
		}
		[Fact]
		[Description("Throws ArgumentException when TopicName is empty")]
		public async Task ProcessBatchAsync_EmptyTopicName_ThrowsArgumentException()
		{
			// Arrange
			var manager = new ConsumerManager(_mockPulsarClient);
			var consumerData = new ConsumerData
			{
				ServiceUrl = Models.TestConstantValues.BROKER_URL,
				SubscriptionName = "test-sub",
				TopicName = "",
				BatchSize = 10,
				MaxNumBytes = 1000,
				TimeoutMs = 5000
			};

			// Act & Assert
			var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
				manager.ProcessBatchAsync(consumerData));
			Assert.Contains("Topic name is required", exception.Message);
		}
		[Fact]
		[Description("Throws ArgumentException when BatchSize is zero")]
		public async Task ProcessBatchAsync_ZeroBatchSize_ThrowsArgumentException()
		{
			// Arrange
			var manager = new ConsumerManager(_mockPulsarClient);
			var consumerData = new ConsumerData
			{
				ServiceUrl = Models.TestConstantValues.BROKER_URL,
				SubscriptionName = "test-sub",
				TopicName = "test-topic",
				BatchSize = 0,
				MaxNumBytes = 1000,
				TimeoutMs = 5000
			};

			// Act & Assert
			var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
				manager.ProcessBatchAsync(consumerData));
			Assert.Contains("Batch size must be positive", exception.Message);
		}
		[Fact]
		[Description("Throws ArgumentException when MaxNumBytes is zero")]
		public async Task ProcessBatchAsync_ZeroMaxNumBytes_ThrowsArgumentException()
		{
			// Arrange
			var manager = new ConsumerManager(_mockPulsarClient);
			var consumerData = new ConsumerData
			{
				ServiceUrl = Models.TestConstantValues.BROKER_URL,
				SubscriptionName = "test-sub",
				TopicName = "test-topic",
				BatchSize = 10,
				MaxNumBytes = 0,
				TimeoutMs = 5000
			};

			// Act & Assert
			var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
				manager.ProcessBatchAsync(consumerData));
			Assert.Contains("Max number of bytes must be positive", exception.Message);
		}
		[Fact]
		[Description("Throws ArgumentException when TimeoutMs is negative")]
		public async Task ProcessBatchAsync_NegativeTimeoutMs_ThrowsArgumentException()
		{
			// Arrange
			var manager = new ConsumerManager(_mockPulsarClient);
			var consumerData = new ConsumerData
			{
				ServiceUrl = Models.TestConstantValues.BROKER_URL,
				SubscriptionName = "test-sub",
				TopicName = "test-topic",
				BatchSize = 10,
				MaxNumBytes = 1000,
				TimeoutMs = -1
			};

			// Act & Assert
			var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
				manager.ProcessBatchAsync(consumerData));
			Assert.Contains("Timeout must be non-negative", exception.Message);
		}

		[Fact]
		[Description("Successfully processes a batch of messages and acknowledges them cumulatively")]
		public async Task ProcessBatchAsync_ValidSetupWithBatch_ReturnsProcessedMessages()
		{
			// Arrange
			var mockPulsarClient = Substitute.For<IPulsarClient>();
			var mockConsumer = Substitute.For<IConsumer<byte[]>>();

			var messages = new List<IMessage<byte[]>>
			{
				Substitute.For<IMessage<byte[]>>(),
				Substitute.For<IMessage<byte[]>>(),
				Substitute.For<IMessage<byte[]>>()
			};
			messages[0].Data.Returns(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("Message 1")));
			messages[0].MessageId.Returns(new MessageId(1, 1, -1, 0, null));
			messages[0].PublishTime.Returns((ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
			messages[1].Data.Returns(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("Message 2")));
			messages[1].MessageId.Returns(new MessageId(2, 2, -1, 0, null));
			messages[1].PublishTime.Returns((ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1);
			messages[2].Data.Returns(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("Message 3")));
			messages[2].MessageId.Returns(new MessageId(3, 3, -1, 0, null));
			messages[2].PublishTime.Returns((ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2);

			// Mock message consumption
			var callCount = 0;
			mockConsumer.Receive(Arg.Any<CancellationToken>()).Returns(
				callInfo =>
				{
					callCount++;
					if (callCount <= 3)
						return ValueTask.FromResult(messages[callCount - 1]);
					return ValueTask.FromResult<IMessage<byte[]>>(null); // Stop after 3 messages
				});
			mockConsumer.Topic.Returns("test-topic");
			mockConsumer.SubscriptionName.Returns("test-sub");
			mockConsumer.AcknowledgeCumulative(Arg.Any<MessageId>(), Arg.Any<CancellationToken>()).Returns(ValueTask.CompletedTask);

			var manager = new ConsumerManager(mockPulsarClient);
			SetPrivateField(manager, "_client", mockPulsarClient);
			SetPrivateField(manager, "_consumer", mockConsumer);

			var consumerData = new ConsumerData
			{
				ServiceUrl = Models.TestConstantValues.BROKER_URL,
				SubscriptionName = "test-sub",
				TopicName = "test-topic",
				BatchSize = 3, // Limit to 3 messages
				MaxNumBytes = 1000,
				TimeoutMs = 5000,
				SubscriptionType = SubscriptionType.Exclusive // Non-Shared for cumulative ack
			};

			using var cts = new CancellationTokenSource();

			// Act
			var result = await manager.ProcessBatchAsync(consumerData, cts.Token);

			// Assert
			Assert.Equal(3, result.Count);
			Assert.Equal("Message 1", result[0].Data);
			Assert.Equal("Message 2", result[1].Data);
			Assert.Equal("Message 3", result[2].Data);
			Assert.Equal("test-topic", result[0].Topic);
			Assert.Equal(messages[2].MessageId, result[2].MessageId);
			await mockConsumer.Received(1).AcknowledgeCumulative(
				Arg.Is<MessageId>(id => id.Equals(messages[2].MessageId)),
				Arg.Any<CancellationToken>());
		}


		[Fact]
		[Description("Processes messages until timeout is reached and acknowledges them cumulatively")]
		public async Task ProcessBatchAsync_TimeoutReached_ReturnsPartialBatch()
		{
			// Arrange
			var mockPulsarClient = Substitute.For<IPulsarClient>();
			var mockConsumer = Substitute.For<IConsumer<byte[]>>();

			var messages = new List<IMessage<byte[]>>
	{
		Substitute.For<IMessage<byte[]>>(),
		Substitute.For<IMessage<byte[]>>()
	};
			messages[0].Data.Returns(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("Message 1")));
			messages[0].MessageId.Returns(new MessageId(1, 1, -1, 0, null));
			messages[0].PublishTime.Returns((ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
			messages[1].Data.Returns(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("Message 2")));
			messages[1].MessageId.Returns(new MessageId(2, 2, -1, 0, null));
			messages[1].PublishTime.Returns((ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1);

			// Mock message consumption with cancellation check
			var callCount = 0;
			mockConsumer.Receive(Arg.Any<CancellationToken>()).Returns(
				callInfo =>
				{
					var token = callInfo.Arg<CancellationToken>();
					if (token.IsCancellationRequested)
						throw new OperationCanceledException("Canceled by timeout");
					callCount++;
					if (callCount == 1)
						return ValueTask.FromResult(messages[0]);
					
					Thread.Sleep(100); // Block for 100ms to ensure timeout hits
					return ValueTask.FromResult(messages[1]);
				});
			mockConsumer.Topic.Returns("test-topic");
			mockConsumer.SubscriptionName.Returns("test-sub");
			mockConsumer.AcknowledgeCumulative(Arg.Any<MessageId>(), Arg.Any<CancellationToken>()).Returns(ValueTask.CompletedTask);

			var manager = new ConsumerManager(mockPulsarClient);
			SetPrivateField(manager, "_client", mockPulsarClient);
			SetPrivateField(manager, "_consumer", mockConsumer);

			var consumerData = new ConsumerData
			{
				ServiceUrl = Models.TestConstantValues.BROKER_URL,
				SubscriptionName = "test-sub",
				TopicName = "test-topic",
				BatchSize = 10, 
				MaxNumBytes = 1000,
				TimeoutMs = 50, 
				SubscriptionType = SubscriptionType.Exclusive // Non-Shared for cumulative ack
			};

			using var cts = new CancellationTokenSource();

			// Act
			var result = await manager.ProcessBatchAsync(consumerData, cts.Token);

			// Assert
			Assert.Equal(1, result.Count); 
			Assert.Equal("Message 1", result[0].Data);
			Assert.Equal("test-topic", result[0].Topic);
			await mockConsumer.Received(1).AcknowledgeCumulative(
				Arg.Is<MessageId>(id => id.Equals(result.Last().MessageId)),
				Arg.Any<CancellationToken>());
		}

		[Fact]
		[Description("Successfully processes a batch with Shared subscription and acknowledges individually")]
		public async Task ProcessBatchAsync_SharedSubscription_ReturnsProcessedMessagesWithIndividualAck()
		{
			// Arrange
			var mockPulsarClient = Substitute.For<IPulsarClient>();
			var mockConsumer = Substitute.For<IConsumer<byte[]>>();

			var messages = new List<IMessage<byte[]>>
			{
				Substitute.For<IMessage<byte[]>>(),
				Substitute.For<IMessage<byte[]>>(),
				Substitute.For<IMessage<byte[]>>()
			};
			messages[0].Data.Returns(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("Message 1")));
			messages[0].MessageId.Returns(new MessageId(1, 1, -1, 0, null));
			messages[0].PublishTime.Returns((ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
			messages[1].Data.Returns(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("Message 2")));
			messages[1].MessageId.Returns(new MessageId(2, 2, -1, 0, null));
			messages[1].PublishTime.Returns((ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1);
			messages[2].Data.Returns(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("Message 3")));
			messages[2].MessageId.Returns(new MessageId(3, 3, -1, 0, null));
			messages[2].PublishTime.Returns((ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2);

			// Mock message consumption
			var callCount = 0;
			mockConsumer.Receive(Arg.Any<CancellationToken>()).Returns(
				callInfo =>
				{
					var token = callInfo.Arg<CancellationToken>();
					if (token.IsCancellationRequested)
						throw new OperationCanceledException("Canceled");
					callCount++;
					if (callCount <= 3)
						return ValueTask.FromResult(messages[callCount - 1]);
					return ValueTask.FromResult<IMessage<byte[]>>(null); // Stop after 3 messages without throwing
				});
			mockConsumer.Topic.Returns("test-topic");
			mockConsumer.SubscriptionName.Returns("test-sub");
			mockConsumer.Acknowledge(Arg.Any<List<MessageId>>(), Arg.Any<CancellationToken>()).Returns(ValueTask.CompletedTask);

			var manager = new ConsumerManager(mockPulsarClient);
			SetPrivateField(manager, "_client", mockPulsarClient);
			SetPrivateField(manager, "_consumer", mockConsumer);

			var consumerData = new ConsumerData
			{
				ServiceUrl = Models.TestConstantValues.BROKER_URL,
				SubscriptionName = "test-sub",
				TopicName = "test-topic",
				BatchSize = 3, // Match the number of messages
				MaxNumBytes = 1000,
				TimeoutMs = 5000, // Long enough to process all
				SubscriptionType = SubscriptionType.Shared // Triggers individual ack
			};

			using var cts = new CancellationTokenSource();

			// Act
			var result = await manager.ProcessBatchAsync(consumerData, cts.Token);

			// Assert
			Assert.Equal(3, result.Count);
			Assert.Equal("Message 1", result[0].Data);
			Assert.Equal("Message 2", result[1].Data);
			Assert.Equal("Message 3", result[2].Data);
			Assert.Equal("test-topic", result[0].Topic);
			await mockConsumer.Received(1).Acknowledge(
				Arg.Is<List<MessageId>>(ids => ids.Count == 3 &&
					ids[0].Equals(messages[0].MessageId) &&
					ids[1].Equals(messages[1].MessageId) &&
					ids[2].Equals(messages[2].MessageId)),
				Arg.Any<CancellationToken>());
			await mockConsumer.DidNotReceive().AcknowledgeCumulative(
				Arg.Any<MessageId>(),
				Arg.Any<CancellationToken>());
		}

		[Fact]
		[Description("Processes a batch with Shared subscription, fails to acknowledge, and still returns messages")]
		public async Task ProcessBatchAsync_AcknowledgmentFails_ReturnsProcessedMessages()
		{
			// Arrange
			var mockPulsarClient = Substitute.For<IPulsarClient>();
			var mockConsumer = Substitute.For<IConsumer<byte[]>>();

			var messages = new List<IMessage<byte[]>>
	{
		Substitute.For<IMessage<byte[]>>(),
		Substitute.For<IMessage<byte[]>>()
	};
			messages[0].Data.Returns(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("Message 1")));
			messages[0].MessageId.Returns(new MessageId(1, 1, -1, 0, null));
			messages[0].PublishTime.Returns((ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
			messages[1].Data.Returns(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("Message 2")));
			messages[1].MessageId.Returns(new MessageId(2, 2, -1, 0, null));
			messages[1].PublishTime.Returns((ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1);

			// Mock message consumption
			var callCount = 0;
			mockConsumer.Receive(Arg.Any<CancellationToken>()).Returns(
				callInfo =>
				{
					var token = callInfo.Arg<CancellationToken>();
					if (token.IsCancellationRequested)
						throw new OperationCanceledException("Canceled");
					callCount++;
					if (callCount <= 2)
						return ValueTask.FromResult(messages[callCount - 1]);
					return ValueTask.FromResult<IMessage<byte[]>>(null); // Stop after 2 messages
				});
			mockConsumer.Topic.Returns("test-topic");
			mockConsumer.SubscriptionName.Returns("test-sub");
			mockConsumer.Acknowledge(Arg.Any<List<MessageId>>(), Arg.Any<CancellationToken>())
				.Returns(_ => throw new Exception("Acknowledgment error"));

			var manager = new ConsumerManager(mockPulsarClient);
			SetPrivateField(manager, "_client", mockPulsarClient);
			SetPrivateField(manager, "_consumer", mockConsumer);

			var consumerData = new ConsumerData
			{
				ServiceUrl = Models.TestConstantValues.BROKER_URL,
				SubscriptionName = "test-sub",
				TopicName = "test-topic",
				BatchSize = 2, // Match the number of messages
				MaxNumBytes = 1000,
				TimeoutMs = 5000, // Long enough to process all
				SubscriptionType = SubscriptionType.Shared // Triggers individual ack
			};

			using var cts = new CancellationTokenSource();

			// Act
			var result = await manager.ProcessBatchAsync(consumerData, cts.Token);

			// Assert
			Assert.Equal(2, result.Count);
			Assert.Equal("Message 1", result[0].Data);
			Assert.Equal("Message 2", result[1].Data);
			Assert.Equal("test-topic", result[0].Topic);
			await mockConsumer.Received(1).Acknowledge(
				Arg.Is<List<MessageId>>(ids => ids.Count == 2 &&
					ids[0].Equals(messages[0].MessageId) &&
					ids[1].Equals(messages[1].MessageId)),
				Arg.Any<CancellationToken>());
			await mockConsumer.DidNotReceive().AcknowledgeCumulative(
				Arg.Any<MessageId>(),
				Arg.Any<CancellationToken>());
		}

		[Fact]
		[Description("Disposes both consumer and client when DisposeAsync is called")]
		public async Task DisposeAsync_WithConsumerAndClient_DisposesBoth()
		{
			// Arrange
			var mockPulsarClient = Substitute.For<IPulsarClient>();
			var mockConsumer = Substitute.For<IConsumer<byte[]>>();
			var manager = new ConsumerManager(mockPulsarClient);

			mockConsumer.DisposeAsync().Returns(ValueTask.CompletedTask);
			mockPulsarClient.DisposeAsync().Returns(ValueTask.CompletedTask);

			SetPrivateField(manager, "_consumer", mockConsumer);
			SetPrivateField(manager, "_client", mockPulsarClient);

			// Act
			await manager.DisposeAsync();

			// Assert
			await mockConsumer.Received(1).DisposeAsync();
			await mockPulsarClient.Received(1).DisposeAsync();
			Assert.Null(GetPrivateField<IConsumer<byte[]>>(manager, "_consumer"));
			Assert.Null(GetPrivateField<IPulsarClient>(manager, "_client"));
		}

	}
}

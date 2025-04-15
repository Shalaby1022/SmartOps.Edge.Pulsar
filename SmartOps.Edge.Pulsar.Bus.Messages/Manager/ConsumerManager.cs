using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using SmartOps.Edge.Pulsar.BaseClasses.Contracts;
using SmartOps.Edge.Pulsar.BaseClasses.Models;
using System.Buffers;
using System.Text;


namespace SmartOps.Edge.Pulsar.Bus.Messages.Manager
{
	public class ConsumerManager : IConsumersManager
	{
		private IPulsarClient? _client;
		private IConsumer<byte[]>? _consumer;
		private bool _stopSubs = false;
		private string? _subscriptionName;
		private readonly Action<IConsumer<byte[]>, Exception>? _errorHandler;
		private readonly SubscriptionType _subscriptionType;
		private readonly uint _messagePrefetchCount;

		// Public constructor for normal use
		public ConsumerManager(
			Action<IConsumer<byte[]>, Exception>? errorHandler = null,
			SubscriptionType subscriptionType = SubscriptionType.Exclusive,
			uint messagePrefetchCount = 1000)
		{
			_errorHandler = errorHandler;
			_subscriptionType = subscriptionType;
			_messagePrefetchCount = messagePrefetchCount > 0 ? messagePrefetchCount : throw new ArgumentException("Message prefetch count must be positive.");
		}

		// constructor for testing with mock client
		public ConsumerManager(
			IPulsarClient client,
			Action<IConsumer<byte[]>, Exception>? errorHandler = null,
			SubscriptionType subscriptionType = SubscriptionType.Exclusive,
			uint messagePrefetchCount = 1000)
		{
			_client = client;
			_errorHandler = errorHandler;
			_subscriptionType = subscriptionType;
			_messagePrefetchCount = messagePrefetchCount > 0 ? messagePrefetchCount : throw new ArgumentException("Message prefetch count must be positive.");
		}


		/// <summary>
		/// Initializes the connection to a Pulsar broker
		/// </summary>
		/// <param name="consumerData">The configuration data for the consumer, including the service URL and subscription name.</param>
		/// <param name="errorHandler">A callback function to handle errors that occur during connection or consumer operation.</param>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="consumerData"/> is null.</exception>
		/// <exception cref="ArgumentException">Thrown when <paramref name="consumerData.SubscriptionName"/> is null or empty.</exception>
		/// <exception cref="UriFormatException">Thrown when <paramref name="consumerData.ServiceUrl"/> is not a valid URI.</exception>
		/// <exception cref="PulsarClientException">Thrown when the connection to the Pulsar broker fails (e.g., network issues or broker unavailable).</exception>

		public void ConnectConsumer(ConsumerData consumerData, Action<IConsumer<byte[]>, Exception> errorHandler)
		{
			if (consumerData == null) throw new ArgumentNullException(nameof(consumerData));
			if (string.IsNullOrEmpty(consumerData.SubscriptionName)) throw new ArgumentException("SubscriptionName is required");

			try
			{
				if (_client == null)
				{
					_client = PulsarClient.Builder()
						.ServiceUrl(new Uri(consumerData.ServiceUrl))
						.Build();
				}
				_subscriptionName = consumerData.SubscriptionName;
				Console.WriteLine($"[INFO] Client connected to {consumerData.ServiceUrl}");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"[ERROR] Failed to connect client: {ex.Message}");
				throw;
			}
		}

		/// <summary>
		/// Subscribes to a Pulsar topic asynchronously and consumes messages, passing each message or error to the provided callback.
		/// </summary>
		/// <param name="topic">The fully qualified topic name to subscribe to (e.g., "persistent://public/default/test-topic").</param>
		/// <param name="callback">An asynchronous callback function that processes each consumed message or handles errors.</param>
		/// <param name="cancellationToken">A token to cancel the subscription and message consumption process.</param>
		/// <returns>A <see cref="Task"/> that completes when the subscription ends (e.g., canceled or errored).</returns>
		/// <exception cref="InvalidOperationException">Thrown when the Pulsar client is not connected (i.e., <see cref="ConnectConsumer"/> has not been called).</exception>
		/// <exception cref="ArgumentException">Thrown when <paramref name="topic"/> is null or empty.</exception>
		/// <exception cref="PulsarClientException">Thrown when subscription or message consumption fails due to broker issues.</exception>
		public async Task SubscribeAsync(string topic, Func<SubscribeMessage<string>, Task> callback, CancellationToken cancellationToken)
		{
			if (_client == null) throw new InvalidOperationException("Client not connected.");
			if (string.IsNullOrEmpty(topic)) throw new ArgumentException("Topic is required");

			try
			{
				if (_consumer == null || _consumer.Topic != topic)
				{
					if (_consumer != null)
					{
						await _consumer.DisposeAsync();
						Console.WriteLine($"[INFO] Previous consumer disposed");
					}

					var consumerBuilder = _client.NewConsumer(Schema.ByteArray)
						.SubscriptionName(_subscriptionName)
						.Topic(topic)
						.InitialPosition(SubscriptionInitialPosition.Earliest)
						.SubscriptionType(_subscriptionType)
						.MessagePrefetchCount(_messagePrefetchCount);

					if (_errorHandler != null)
					{
						consumerBuilder.StateChangedHandler(new StateChangedHandler(_errorHandler));
					}

					_consumer = consumerBuilder.Create();
					Console.WriteLine($"[INFO] Subscribed to topic: {topic} with {_subscriptionType}");
				}

				_stopSubs = false;

				await foreach (var message in _consumer.Messages().WithCancellation(cancellationToken))
				{
					if (_stopSubs || cancellationToken.IsCancellationRequested) break;

					var content = Encoding.UTF8.GetString(message.Data.ToArray());
					var subscribeMessage = new SubscribeMessage<string>
					{
						Data = content,
						Topic = _consumer.Topic,
						MessageId = message.MessageId,
						UnixTimeStampMs = (long)message.PublishTime
					};

					Console.WriteLine($"[INFO] Consumed message from {_consumer.Topic}");
					await callback(subscribeMessage);
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"[ERROR] Failed to subscribe to topic: {ex.Message}");
				var errorMessage = new SubscribeMessage<string>
				{
					ExceptionMessage = ex.Message,
					ErrorCode = ex is OperationCanceledException ? "REQUESTED_ABORTED" : "SUBSCRIBE_ERROR"
				};
				await callback(errorMessage);
			}
		}

		/// <summary>
		/// Asynchronously processes a batch of messages from a Pulsar topic, adhering to specified size, byte, and timeout constraints.
		/// </summary>
		/// <param name="consumerData">The configuration data for the consumer, including service URL, subscription name, topic name, batch size, max bytes, and timeout.</param>
		/// <param name="cancellationToken">A token to cancel the batch processing operation. Defaults to <see cref="CancellationToken.None"/>.</param>
		/// <returns>A <see cref="Task"/> that resolves to a list of processed <see cref="SubscribeMessage{string}"/> objects containing message data and metadata.</returns>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="consumerData"/> is null.</exception>
		/// <exception cref="ArgumentException">Thrown when required fields in <paramref name="consumerData"/> (ServiceUrl, SubscriptionName, TopicName) are null or empty, or when BatchSize, MaxNumBytes, or TimeoutMs have invalid values.</exception>
		/// <exception cref="PulsarClientException">Thrown when connection to the Pulsar broker fails or message consumption encounters broker-related issues.</exception>
		/// <exception cref="OperationCanceledException">Thrown when the operation is canceled via <paramref name="cancellationToken"/> or the timeout specified in <paramref name="consumerData.TimeoutMs"/> is reached.</exception>

		public async Task<List<SubscribeMessage<string>>> ProcessBatchAsync(ConsumerData consumerData, CancellationToken cancellationToken = default)
		{
			if (consumerData == null) throw new ArgumentNullException(nameof(consumerData));
			if (string.IsNullOrEmpty(consumerData.ServiceUrl)) throw new ArgumentException("Service URL is required", nameof(consumerData.ServiceUrl));
			if (string.IsNullOrEmpty(consumerData.SubscriptionName)) throw new ArgumentException("Subscription name is required", nameof(consumerData.SubscriptionName));
			if (string.IsNullOrEmpty(consumerData.TopicName)) throw new ArgumentException("Topic name is required", nameof(consumerData.TopicName));
			if (consumerData.BatchSize <= 0) throw new ArgumentException("Batch size must be positive", nameof(consumerData.BatchSize));
			if (consumerData.MaxNumBytes <= 0) throw new ArgumentException("Max number of bytes must be positive", nameof(consumerData.MaxNumBytes));
			if (consumerData.TimeoutMs < 0) throw new ArgumentException("Timeout must be non-negative", nameof(consumerData.TimeoutMs));

			try
			{
				if (_client == null)
				{
					ConnectConsumer(consumerData, _errorHandler ?? ((consumer, ex) => Console.WriteLine($"[ERROR] Consumer error: {ex.Message}")));
				}

				if (_consumer == null || _consumer.Topic != consumerData.TopicName || _consumer.SubscriptionName != consumerData.SubscriptionName)
				{
					if (_consumer != null)
					{
						await _consumer.DisposeAsync();
						Console.WriteLine("[INFO] Previous consumer disposed");
					}

					var consumerBuilder = _client.NewConsumer(Schema.ByteArray)
						.SubscriptionName(consumerData.SubscriptionName)
						.Topic(consumerData.TopicName)
						.InitialPosition(SubscriptionInitialPosition.Latest)
						.SubscriptionType(consumerData.SubscriptionType)
						.MessagePrefetchCount((uint)Math.Min(consumerData.BatchSize, _messagePrefetchCount));

					if (_errorHandler != null)
					{
						consumerBuilder.StateChangedHandler(new StateChangedHandler(_errorHandler));
					}

					_consumer = consumerBuilder.Create();
					Console.WriteLine($"[INFO] Subscribed to topic: {consumerData.TopicName} with subscription: {consumerData.SubscriptionName}");
				}

				if (consumerData.SubscriptionType == SubscriptionType.Shared)
				{
					Console.WriteLine("[WARN] Cumulative acknowledgment not supported with Shared subscription; using individual acks");
				}

				var messages = new List<SubscribeMessage<string>>(consumerData.BatchSize);
				int effectiveBatchSize = consumerData.BatchSize;
				long totalBytes = 0;
				const long memoryThreshold = 1024 * 1024 * 500; // 500MB , I just add it to prevent memory overload
				using var timeoutCts = new CancellationTokenSource(consumerData.TimeoutMs);
				using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

				await foreach (var message in _consumer.Messages().WithCancellation(linkedCts.Token))
				{
					if (linkedCts.Token.IsCancellationRequested)
					{
						Console.WriteLine("[INFO] Batch processing stopped: " +
							(timeoutCts.IsCancellationRequested ? "timeout reached" : "canceled by caller"));
						break;
					}
					if (messages.Count >= effectiveBatchSize)
					{
						Console.WriteLine($"[INFO] Reached effective batch size limit: {effectiveBatchSize}");
						break;
					}

					long memoryUsed = GC.GetTotalMemory(false);
					if (memoryUsed > memoryThreshold)
					{
						effectiveBatchSize = Math.Max(1, effectiveBatchSize / 2);
						Console.WriteLine($"[WARN] Memory usage high ({memoryUsed / (1024 * 1024)}MB > {memoryThreshold / (1024 * 1024)}MB), reduced effective batch size to {effectiveBatchSize}");
						break;
					}

					var contentBytes = message.Data.ToArray();
					var content = Encoding.UTF8.GetString(contentBytes);
					totalBytes += contentBytes.Length;
					if (totalBytes > consumerData.MaxNumBytes)
					{
						Console.WriteLine($"[INFO] Exceeded max bytes ({totalBytes} > {consumerData.MaxNumBytes}), stopping batch");
						break;
					}

					if (string.IsNullOrEmpty(content))
					{
						Console.WriteLine("[WARN] Empty message detected, skipping");
						continue;
					}

					var processedContent = content.Trim(); // left for a debate with hady coz it's depend on nature of the message that "pulsar may detect leading and trailing whitespace characters as noise from producer" but the question is what if the message payload has trailed whitespace for example this will transalte it into a very diff msg.  
					var subscribeMessage = new SubscribeMessage<string>
					{
						Data = processedContent,
						Topic = _consumer.Topic,
						MessageId = message.MessageId,
						UnixTimeStampMs = (long)message.PublishTime
					};

					messages.Add(subscribeMessage);
					Console.WriteLine($"[INFO] Processed message {messages.Count}/{effectiveBatchSize} from {_consumer.Topic}, total bytes: {totalBytes}");
				}

				if (messages.Count > 0)
				{
					try
					{
						// CHANGED: Use consumerData.SubscriptionType for acknowledgment
						if (consumerData.SubscriptionType != SubscriptionType.Shared)
						{
							var lastMessageId = messages.Last().MessageId;
							await _consumer.AcknowledgeCumulative(lastMessageId, cancellationToken);
							Console.WriteLine($"[INFO] Acknowledged {messages.Count} messages cumulatively up to {lastMessageId}");
						}
						else
						{
							var messageIds = messages.Select(m => m.MessageId).ToList();
							await _consumer.Acknowledge(messageIds, cancellationToken);
							Console.WriteLine($"[INFO] Individually acknowledged {messages.Count} messages");
						}
					}
					catch (Exception ackEx)
					{
						Console.WriteLine($"[ERROR] Acknowledgment failed: {ackEx.Message}. Returning processed messages anyway");
					}
				}
				else
				{
					Console.WriteLine("[INFO] No messages processed in this batch");
				}

				return messages;
			}
			catch (Exception ex)
			{
				Console.WriteLine($"[ERROR] Failed to process batch: {ex.Message}");
				throw;
			}
		}

		/// <summary>
		/// Asynchronously acknowledges a list of messages as processed by the consumer, informing the Pulsar broker they do not need to be redelivered.
		/// </summary>
		/// <param name="messageIds">A list of message identifiers to acknowledge, marking them as successfully processed.</param>
		/// <returns>A <see cref="Task{TResult}"/> that resolves to a <see cref="BaseResponse"/> indicating whether the acknowledgment succeeded or failed.</returns>
		/// <exception cref="InvalidOperationException">Thrown when the consumer is not connected (i.e., <see cref="SubscribeAsync"/> has not been called or the consumer was disposed).</exception>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="messageIds"/> is null (handled implicitly by <see cref="IConsumer{T}.Acknowledge"/>).</exception>
		/// <exception cref="PulsarClientException">Thrown when the acknowledgment fails due to broker issues or consumer state (e.g., closed connection).</exception>
		public async Task<BaseResponse> AcknowledgeAsync(List<MessageId> messageIds)
		{
			var response = new BaseResponse();
			try
			{
				if (_consumer == null) throw new InvalidOperationException("Consumer not connected.");
				await _consumer.Acknowledge(messageIds, CancellationToken.None);
				response.IsSuccess = true;
				response.Message = "Messages acknowledged successfully";
				Console.WriteLine($"[INFO] Acknowledged {messageIds.Count} messages");
			}
			catch (Exception ex)
			{
				response.IsSuccess = false;
				response.Message = $"Failed to acknowledge messages: {ex.Message}";
				response.ErrorCode = "ACK_ERROR";
				Console.WriteLine($"[ERROR] Acknowledgment failed: {ex.Message}");
			}
			return response;
		}
		/// <summary>
		/// Asynchronously acknowledges all messages up to and including the specified message ID in a cumulative manner, restricted to non-shared subscription types.
		/// </summary>
		/// <param name="messageId">The message identifier up to which all prior messages in the subscription will be acknowledged.</param>
		/// <returns>A <see cref="Task{TResult}"/> that resolves to a <see cref="BaseResponse"/> indicating whether the cumulative acknowledgment succeeded or failed.</returns>
		/// <exception cref="InvalidOperationException">Thrown when the consumer is not connected (i.e., <see cref="SubscribeAsync"/> has not been called or the consumer was disposed),
		/// or if the subscription type is <see cref="SubscriptionType.Shared"/>, which does not support cumulative acknowledgment.</exception>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="messageId"/> is null (handled implicitly by <see cref="IConsumer{T}.AcknowledgeCumulative"/>).</exception>
		/// <exception cref="PulsarClientException">Thrown when the cumulative acknowledgment fails due to broker issues or consumer state (e.g., closed connection).</exception>
		public async Task<BaseResponse> AcknowledgeCumulativeAsync(MessageId messageId)
		{
			var response = new BaseResponse();
			try
			{
				if (_consumer == null) throw new InvalidOperationException("Consumer not connected.");
				if (_subscriptionType == SubscriptionType.Shared)
					throw new InvalidOperationException("Cumulative acknowledgment is not supported in Shared subscription type.");
				await _consumer.AcknowledgeCumulative(messageId, CancellationToken.None);
				response.IsSuccess = true;
				response.Message = "Messages acknowledged cumulatively";
				Console.WriteLine($"[INFO] Cumulatively acknowledged up to message ID: {messageId}");
			}
			catch (Exception ex)
			{
				response.IsSuccess = false;
				response.Message = $"Failed to acknowledge cumulatively: {ex.Message}";
				response.ErrorCode = "ACK_CUMULATIVE_ERROR";
				Console.WriteLine($"[ERROR] Cumulative acknowledgment failed: {ex.Message}");
			}
			return response;
		}


		/// <summary>
		/// Asynchronously requests the Pulsar broker to redeliver specific unacknowledged messages identified by their message IDs,
		/// allowing the consumer to retry processing them.
		/// </summary>
		/// <param name="messageIds">A collection of message identifiers for the unacknowledged messages to redeliver.</param>
		/// <returns>A <see cref="Task{TResult}"/> that resolves to a <see cref="BaseResponse"/> indicating whether the redelivery request succeeded or failed.</returns>
		/// <exception cref="InvalidOperationException">Thrown when the consumer is not connected (i.e., <see cref="SubscribeAsync"/> has not been called or the consumer was disposed).</exception>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="messageIds"/> is null (handled implicitly by <see cref="IConsumer{T}.RedeliverUnacknowledgedMessages"/>).</exception>
		/// <exception cref="PulsarClientException">Thrown when the redelivery request fails due to broker issues or consumer state (e.g., closed connection).</exception>
		public async Task<BaseResponse> RedeliverUnacknowledgedMessagesAsync(IEnumerable<MessageId> messageIds)
		{
			var response = new BaseResponse();
			try
			{
				if (_consumer == null) throw new InvalidOperationException("Consumer not connected.");
				await _consumer.RedeliverUnacknowledgedMessages(messageIds, CancellationToken.None);
				response.IsSuccess = true;
				response.Message = "Requested redelivery of unacknowledged messages";
				Console.WriteLine($"[INFO] Requested redelivery for {messageIds.Count()} message(s)");
			}
			catch (Exception ex)
			{
				response.IsSuccess = false;
				response.Message = $"Failed to redeliver messages: {ex.Message}";
				response.ErrorCode = "REDELIVER_ERROR";
				Console.WriteLine($"[ERROR] Redelivery failed: {ex.Message}");
			}
			return response;
		}

		/// <summary>
		/// Asynchronously requests the Pulsar broker to redeliver all unacknowledged messages for the current consumer’s subscription,
		/// allowing a full retry of messages that have not been processed successfully.
		/// </summary>
		/// <returns>A <see cref="Task{TResult}"/> that resolves to a <see cref="BaseResponse"/> indicating whether the redelivery request succeeded or failed.</returns>
		/// <exception cref="InvalidOperationException">Thrown when the consumer is not connected (i.e., <see cref="SubscribeAsync"/> has not been called or the consumer was disposed).</exception>
		/// <exception cref="PulsarClientException">Thrown when the redelivery request fails due to broker issues or consumer state (e.g., closed connection).</exception>
		/// <remarks>
		/// This method requires an active consumer established via <see cref="SubscribeAsync"/>. Unlike <see cref="RedeliverUnacknowledgedMessagesAsync"/>,
		/// it does not target specific messages but redelivers all unacknowledged messages in the subscription. On success, the <see cref="BaseResponse"/>
		/// has <c>IsSuccess</c> set to <c>true</c> and a message indicating the redelivery request was sent. On failure, it includes an error message and sets
		/// <c>ErrorCode</c> to "REDELIVER_ALL_ERROR". The redelivered messages will be reprocessed through the existing subscription.
		/// </remarks>
		public async Task<BaseResponse> RedeliverAllUnacknowledgedMessagesAsync()
		{
			var response = new BaseResponse();
			try
			{
				if (_consumer == null) throw new InvalidOperationException("Consumer not connected.");
				await _consumer.RedeliverUnacknowledgedMessages(CancellationToken.None);
				response.IsSuccess = true;
				response.Message = "Requested redelivery of all unacknowledged messages";
				Console.WriteLine("[INFO] Requested redelivery of all unacknowledged messages");
			}
			catch (Exception ex)
			{
				response.IsSuccess = false;
				response.Message = $"Failed to redeliver all messages: {ex.Message}";
				response.ErrorCode = "REDELIVER_ALL_ERROR";
				Console.WriteLine($"[ERROR] Redelivery of all messages failed: {ex.Message}");
			}
			return response;
		}


		/// <summary>
		/// Asynchronously disposes of the <see cref="ConsumerManager"/> resources, closing and releasing the consumer and client connections to the Pulsar broker.
		/// </summary>
		/// <returns>A <see cref="ValueTask"/> that completes when the disposal process is finished.</returns>
		/// <remarks>
		/// This method safely disposes of the <see cref="IConsumer{T}"/> and <see cref="IPulsarClient"/> instances if they exist, ensuring no resources remain allocated.
		/// It logs the disposal of each component for visibility. After disposal, the consumer and client references are set to <c>null</c> to prevent reuse.
		/// This method should be called when the <see cref="ConsumerManager"/> is no longer needed, typically at the end of its lifecycle or scope.
		/// </remarks>
		public async ValueTask DisposeAsync()
		{
			if (_consumer != null)
			{
				await _consumer.DisposeAsync();
				Console.WriteLine("[INFO] Consumer disposed");
				_consumer = null;
			}
			if (_client != null)
			{
				await _client.DisposeAsync();
				Console.WriteLine("[INFO] Client disposed");
				_client = null;
			}
		}
	}

	/// <summary>
	/// An internal handler that monitors state changes of a Pulsar consumer and invokes an error callback when the consumer enters a faulted state.
	/// Implements <see cref="IHandleStateChanged{T}"/> for <see cref="ConsumerStateChanged"/> events.
	/// </summary>
	internal class StateChangedHandler : IHandleStateChanged<ConsumerStateChanged>
	{
		/// <summary>
		/// The callback to invoke when the consumer faults, receiving the consumer instance and an exception.
		/// </summary>
		private readonly Action<IConsumer<byte[]>, Exception> _errorHandler;

		/// <summary>
		/// Initializes a new instance of <see cref="StateChangedHandler"/> with the specified error callback.
		/// </summary>
		/// <param name="errorHandler">The callback to execute when the consumer enters a faulted state.</param>
		public StateChangedHandler(Action<IConsumer<byte[]>, Exception> errorHandler)
		{
			_errorHandler = errorHandler;
		}


		/// <summary>
		/// Gets the cancellation token for state change handling. Always returns <see cref="CancellationToken.None"/>,
		/// indicating cancellation is not supported.
		/// </summary>
		public CancellationToken CancellationToken => CancellationToken.None;

		/// <summary>
		/// Handles a state change event for the consumer, invoking the error callback if the consumer becomes faulted.
		/// </summary>
		/// <param name="stateChanged">The state change event data, including the consumer and its new state.</param>
		/// <param name="cancellationToken">A cancellation token passed by the caller, ignored in this implementation.</param>
		/// <returns>A <see cref="ValueTask"/> that completes immediately after handling the state change.</returns>
		public ValueTask OnStateChanged(ConsumerStateChanged stateChanged, CancellationToken cancellationToken)
		{
			if (stateChanged.ConsumerState == ConsumerState.Faulted && stateChanged.Consumer is IConsumer<byte[]> consumer)
			{
				_errorHandler(consumer, new Exception($"Consumer faulted: {stateChanged.ConsumerState}"));
			}
			return ValueTask.CompletedTask;
		}
	}
}

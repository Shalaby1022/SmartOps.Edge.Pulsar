using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using SmartOps.Edge.Pulsar.BaseClasses.Contracts;
using SmartOps.Edge.Pulsar.BaseClasses.Models;
using System.Buffers;
using System.Text;


namespace SmartOps.Edge.Pulsar.Messages.Manager
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
		/// Initializes the connection to a Pulsar broker using the specified consumer data and error handler.
		/// This method must be called before subscribing to topics or consuming messages.
		/// </summary>
		/// <param name="consumerData">The configuration data for the consumer, including the service URL and subscription name.</param>
		/// <param name="errorHandler">A callback function to handle errors that occur during connection or consumer operation.</param>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="consumerData"/> is null.</exception>
		/// <exception cref="ArgumentException">Thrown when <paramref name="consumerData.SubscriptionName"/> is null or empty.</exception>
		/// <exception cref="UriFormatException">Thrown when <paramref name="consumerData.ServiceUrl"/> is not a valid URI.</exception>
		/// <exception cref="PulsarClientException">Thrown when the connection to the Pulsar broker fails (e.g., network issues or broker unavailable).</exception>
		/// <remarks>
		/// If a client is already initialized (e.g., via the testing constructor), this method reuses it without creating a new one.
		/// The subscription name is stored for use in subsequent subscription operations.
		/// </remarks>
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
		/// <remarks>
		/// If a consumer already exists for a different topic, it is disposed before creating a new one. The consumer is reused if the topic matches.
		/// Messages are consumed from the latest position by default, and the subscription type and prefetch count are applied from the constructor settings.
		/// Errors during subscription or consumption are passed to the <paramref name="callback"/> as a <see cref="SubscribeMessage{T}"/> with <c>ExceptionMessage</c> and <c>ErrorCode</c> set.
		/// </remarks>
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
						.InitialPosition(SubscriptionInitialPosition.Latest)
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
		/// Asynchronously acknowledges a list of messages as processed by the consumer, informing the Pulsar broker they do not need to be redelivered.
		/// </summary>
		/// <param name="messageIds">A list of message identifiers to acknowledge.</param>
		/// <returns>A <see cref="Task{TResult}"/> that resolves to a <see cref="BaseResponse"/> indicating whether the acknowledgment succeeded or failed.</returns>
		/// <exception cref="InvalidOperationException">Thrown when the consumer is not connected (i.e., <see cref="SubscribeAsync"/> has not been called or the consumer was disposed).</exception>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="messageIds"/> is null (handled implicitly by <see cref="IConsumer{T}.Acknowledge"/>).</exception>
		/// <exception cref="PulsarClientException">Thrown when the acknowledgment fails due to broker issues or consumer state (e.g., closed connection).</exception>
		/// <remarks>
		/// This method requires an active consumer, established via <see cref="SubscribeAsync"/>. On success, the <see cref="BaseResponse"/> has <c>IsSuccess</c> set to <c>true</c>
		/// and a message indicating the number of messages acknowledged. On failure, it includes an error message and sets <c>ErrorCode</c> to "ACK_ERROR".
		/// </remarks>
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
		/// <remarks>
		/// This method requires an active consumer established via <see cref="SubscribeAsync"/>. It targets only the specified <paramref name="messageIds"/>,
		/// unlike <see cref="RedeliverAllUnacknowledgedMessagesAsync"/>, which affects all unacknowledged messages. On success, the <see cref="BaseResponse"/>
		/// has <c>IsSuccess</c> set to <c>true</c> and a message indicating the redelivery request was sent. On failure, it includes an error message and sets
		/// <c>ErrorCode</c> to "REDELIVER_ERROR". The redelivered messages will be reprocessed through the existing subscription.
		/// </remarks>
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
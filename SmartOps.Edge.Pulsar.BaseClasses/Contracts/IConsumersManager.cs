using SmartOps.Edge.Pulsar.BaseClasses.Models;
using DotPulsar; 
using DotPulsar.Abstractions;

namespace SmartOps.Edge.Pulsar.BaseClasses.Contracts
{
	public interface IConsumersManager : IAsyncDisposable
	{
		/// <summary>
		/// Initializes the consumer manager with connection details and an error handler for fault monitoring.
		/// </summary>
		/// <param name="consumerData">The configuration data containing the service URL and subscription name to connect to the messaging system.</param>
		/// <param name="errorHandler">A callback action that receives the consumer instance and an exception when an error occurs, used for monitoring faults.</param>

		void ConnectConsumer(ConsumerData consumerData, Action<IConsumer<byte[]>, Exception> errorHandler);

		/// <summary>
		/// Subscribes to a specified topic asynchronously, consuming messages and passing them to a callback for processing.
		/// </summary>
		/// <param name="topic">The name of the topic to subscribe to, such as "persistent://public/default/test-topic-1".</param>
		/// <param name="callback">An asynchronous function that processes each consumed message, receiving a SubscribeMessage containing message data and metadata.</param>
		/// <param name="cancellationToken">A token used to signal cancellation and stop the subscription process.</param>
		/// <returns>A task representing the asynchronous subscription operation.</returns>

		//async Task SubscribeAsync(string topic, string subscriptionName, SubscriptionType subscriptionType, Func<SubscribeMessage<string>, Task> callback, CancellationToken cancellationToken);

		Task SubscribeAsync(string topic, Func<SubscribeMessage<string>, Task> callback, CancellationToken cancellationToken);

		/// <summary>
		/// Acknowledges a list of message IDs, marking them as processed and removing them from the subscription's backlog.
		/// </summary>
		/// <param name="messageIds">A list of MessageId objects identifying the messages to be acknowledged.</param>
		/// <returns>A task that resolves to a <see cref="BaseResponse"/> indicating the operation's success or failure.</returns>
		Task<BaseResponse> AcknowledgeAsync(List<MessageId> messageIds);

		/// <summary>
		/// Acknowledges all messages up to and including the specified message ID in a cumulative manner.
		/// Restricted to non-shared subscription types.
		/// </summary>
		/// <param name="messageId">The MessageId up to which all prior messages in the subscription will be acknowledged.</param>
		/// <returns>A task that resolves to a <see cref="BaseResponse"/> indicating the operation's success or failure.</returns>
		Task<BaseResponse> AcknowledgeCumulativeAsync(MessageId messageId);

		/// <summary>
		/// Requests redelivery of specific unacknowledged messages identified by their message IDs.
		/// </summary>
		/// <param name="messageIds">A collection of MessageId objects specifying which unacknowledged messages to redeliver.</param>
		/// <returns>A task that resolves to a <see cref="BaseResponse"/> indicating the operation's success or failure.</returns>
		Task<BaseResponse> RedeliverUnacknowledgedMessagesAsync(IEnumerable<MessageId> messageIds);

		/// <summary>
		/// Requests redelivery of all unacknowledged messages for the current subscription.
		/// </summary>
		/// <returns>A task that resolves to a <see cref="BaseResponse"/> indicating the operation's success or failure.</returns>
		Task<BaseResponse> RedeliverAllUnacknowledgedMessagesAsync();

		/// <summary>
		/// Processes a batch of messages based on the configuration in <paramref name="consumerData"/>, returning a list of processed messages.
		/// </summary>
		/// <param name="consumerData">The configuration data containing the service URL, subscription name, topic name, and batch size.</param>
		/// <param name="cancellationToken">A token to cancel the batch processing operation.</param>
		/// <returns>A task that resolves to a list of processed messages wrapped in <see cref="SubscribeMessage{T}"/>.</returns>
		Task<List<SubscribeMessage<string>>> ProcessBatchAsync(ConsumerData consumerData, CancellationToken cancellationToken = default);
	}
}

using SmartOps.Edge.Pulsar.BaseClasses.Models;
using DotPulsar; // For IConsumer<byte[]>
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Extensions;


namespace SmartOps.Edge.Pulsar.BaseClasses.Contracts
{
	public interface IConsumersManager : IAsyncDisposable
	{
		void ConnectConsumer(ConsumerData consumerData, Action<IConsumer<byte[]>, Exception> errorHandler);
		Task SubscribeAsync(string topic, Func<SubscribeMessage<string>, Task> callback, CancellationToken cancellationToken);
		Task<BaseResponse> AcknowledgeAsync(List<MessageId> messageIds); // Use DotPulsar.MessageId
		Task<BaseResponse> AcknowledgeCumulativeAsync(MessageId messageId);
		// New methods added based on ConsumerManager implementation
		Task<BaseResponse> RedeliverUnacknowledgedMessagesAsync(IEnumerable<MessageId> messageIds);
		Task<BaseResponse> RedeliverAllUnacknowledgedMessagesAsync();
	}
}

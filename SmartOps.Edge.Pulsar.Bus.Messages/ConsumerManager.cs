using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using SmartOps.Edge.Pulsar.BaseClasses.Contracts;
using SmartOps.Edge.Pulsar.BaseClasses.Models;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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
		private readonly uint _messagePrefetchCount; // New: Replaces ReceiverQueueSize

		public ConsumerManager(
			Action<IConsumer<byte[]>, Exception>? errorHandler = null,
			SubscriptionType subscriptionType = SubscriptionType.Exclusive,
			uint messagePrefetchCount = 1000) // Default from docs
		{
			_errorHandler = errorHandler;
			_subscriptionType = subscriptionType;
			_messagePrefetchCount = messagePrefetchCount > 0 ? messagePrefetchCount : throw new ArgumentException("Message prefetch count must be positive.");
		}

		public void ConnectConsumer(ConsumerData consumerData, Action<IConsumer<byte[]>, Exception> errorHandler)
		{
			if (consumerData == null) throw new ArgumentNullException(nameof(consumerData));
			if (string.IsNullOrEmpty(consumerData.SubscriptionName)) throw new ArgumentException("SubscriptionName is required");

			try
			{
				_client = PulsarClient.Builder()
					.ServiceUrl(new Uri(consumerData.ServiceUrl))
					.Build();
				_subscriptionName = consumerData.SubscriptionName;
				Console.WriteLine($"[INFO] Client connected to {consumerData.ServiceUrl}");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"[ERROR] Failed to connect client: {ex.Message}");
				throw;
			}
		}

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
						.MessagePrefetchCount(_messagePrefetchCount); // New: Set prefetch count

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
				await callback(errorMessage); // Fixed: Removed HAPPY
			}
		}

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

	internal class StateChangedHandler : IHandleStateChanged<ConsumerStateChanged>
	{
		private readonly Action<IConsumer<byte[]>, Exception> _errorHandler;

		public StateChangedHandler(Action<IConsumer<byte[]>, Exception> errorHandler)
		{
			_errorHandler = errorHandler;
		}

		public CancellationToken CancellationToken => CancellationToken.None;

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
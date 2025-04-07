using Microsoft.Extensions.DependencyInjection;
using SmartOps.Edge.Pulsar.BaseClasses.Contracts;
using SmartOps.Edge.Pulsar.BaseClasses.Models;
using SmartOps.Edge.Pulsar.Messages.Manager;
using DotPulsar;
using DotPulsar.Extensions;
using System.Text;
using SmartOps.Edge.Pulsar.Bus.Messages.Manager;

namespace ConsolePulsarTest
{
	public class Program
	{
		static async Task Main(string[] args)
		{
			#region ServiceRegistration
			var serviceCollection = new ServiceCollection();
			serviceCollection.AddHttpClient();
			serviceCollection.AddSingleton<ITopicManager, TopicManager>();
			serviceCollection.AddSingleton<IProducerManager, PulsarProducerManager>();
			serviceCollection.AddSingleton<IConsumersManager>(provider =>
				new ConsumerManager(
					null,
					SubscriptionType.Shared, // Default remains Shared for other tests
					messagePrefetchCount: 500));
			var serviceProvider = serviceCollection.BuildServiceProvider();
			using var scope = serviceProvider.CreateScope();
			var topicManager = scope.ServiceProvider.GetRequiredService<ITopicManager>();
			var consumerManager = scope.ServiceProvider.GetRequiredService<IConsumersManager>();
			#endregion

			// Test Case 1: Create a new topic with minimal configuration
			Console.WriteLine("Test Case 1: Create new topic with minimal config");
			var topicData1 = new CreateTopicData("pulsar://localhost:6650", "test-topic-1")
			{
				NumPartitions = 1
			};
			var response1 = await topicManager.CreateTopic(topicData1);
			PrintResponse(response1);

			// Test Case 2: Attempt to recreate the same topic
			Console.WriteLine("\nTest Case 2: Attempt to recreate existing topic");
			var topicData2 = new CreateTopicData("pulsar://localhost:6650", "test-topic-1")
			{
				NumPartitions = 1
			};
			var response2 = await topicManager.CreateTopic(topicData2);
			PrintResponse(response2);

			// Test Case 3: Create topic with full configuration
			Console.WriteLine("\nTest Case 3: Create topic with full configuration");
			var topicData3 = new CreateTopicData("pulsar://localhost:6650", "test-topic-2")
			{
				NumPartitions = 3,
				RetentionMins = "86400000",
				RetentionSizeMB = 1024,
				ReplicationFactor = 2
			};
			var response3 = await topicManager.CreateTopic(topicData3);
			PrintResponse(response3);

			// Test Case 4: Test error handling with invalid input (negative partitions)
			Console.WriteLine("\nTest Case 4: Test error handling with invalid input (negative partitions)");
			try
			{
				var topicData4 = new CreateTopicData("pulsar://localhost:6650", "test-topic-3")
				{
					NumPartitions = -1
				};
				var response4 = await topicManager.CreateTopic(topicData4);
				PrintResponse(response4);
			}
			catch (ArgumentException ex)
			{
				Console.WriteLine($"Expected error: {ex.Message}");
			}

			// Test Case 5: Produce and consume with enhanced features
			Console.WriteLine("\nTest Case 5: Produce and consume with enhanced features");
			try
			{
				var consumerData = new ConsumerData { SubscriptionName = "TestSub1" };
				consumerManager.ConnectConsumer(consumerData, (c, e) => Console.WriteLine($"[ERROR] Consumer error: {e.Message}"));

				var producerManager = scope.ServiceProvider.GetRequiredService<IProducerManager>();
				var producerData = new ProducerData
				{
					ServiceUrl = "pulsar://localhost:6650",
					TopicName = "persistent://public/default/test-topic-1",
					ProducerName = "console-test-producer",
					MaxPendingMessages = 1000,
					SendTimeoutMs = 30000,
					EnableBatching = true
				};

				await producerManager.ConnectProducer(producerData);

				await producerManager.PublishAsync("persistent://public/default/test-topic-1", "Message 1: Success", correlationId: Guid.NewGuid().ToString());
				await producerManager.PublishAsync("persistent://public/default/test-topic-1", "Message 2: Retry", correlationId: Guid.NewGuid().ToString());
				await producerManager.PublishAsync("persistent://public/default/test-topic-1", "Message 3: Fail", correlationId: Guid.NewGuid().ToString());
				Console.WriteLine("[INFO] Produced 3 messages to persistent://public/default/test-topic-1");
				await Task.Delay(2000);

				var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
				int messageCount = 0;
				await consumerManager.SubscribeAsync("persistent://public/default/test-topic-1",
					async msg =>
					{
						if (string.IsNullOrEmpty(msg.ExceptionMessage))
						{
							messageCount++;
							Console.WriteLine($"Received: {msg.Data} (Timestamp: {msg.UnixTimeStampMs})");

							if (msg.Data.Contains("Success"))
							{
								await consumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId });
							}
							else if (msg.Data.Contains("Retry"))
							{
								await consumerManager.RedeliverUnacknowledgedMessagesAsync(new List<MessageId> { msg.MessageId });
							}
							else if (msg.Data.Contains("Fail"))
							{
								// Don’t ack; redeliver all unacknowledged later
							}

							if (messageCount >= 3)
							{
								await consumerManager.RedeliverAllUnacknowledgedMessagesAsync();
								cts.Cancel();
							}
						}
						else
						{
							Console.WriteLine($"Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
						}
					}, cts.Token);

				Console.WriteLine("Message consumption test completed.");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Test Case 5 failed: {ex.Message}");
			}

			// Test Case 6: Batch Consuming with Full Criteria and Enhancements
			Console.WriteLine("\nTest Case 6: Batch Consuming with Full Criteria and Enhancements");
			try
			{
				// Ensure consumer is connected
				var consumerDataSetup = new ConsumerData { SubscriptionName = "TestSubBatch" };
				consumerManager.ConnectConsumer(consumerDataSetup, (c, e) => Console.WriteLine($"[ERROR] Consumer error: {e.Message}"));

				// Produce additional messages for batch testing
				var producerManager = scope.ServiceProvider.GetRequiredService<IProducerManager>();
				var producerData = new ProducerData
				{
					ServiceUrl = "pulsar://localhost:6650",
					TopicName = "persistent://public/default/test-topic-1",
					ProducerName = "batch-test-producer",
					MaxPendingMessages = 1000,
					SendTimeoutMs = 30000,
					EnableBatching = true
				};
				await producerManager.ConnectProducer(producerData);

				// Produce 20 small messages and 1 large message
				for (int i = 1; i <= 20; i++)
				{
					await producerManager.PublishAsync("persistent://public/default/test-topic-1", $"Batch Message {i}", correlationId: Guid.NewGuid().ToString());
				}
				await producerManager.PublishAsync("persistent://public/default/test-topic-1", new string('X', 2 * 1024 * 1024), correlationId: Guid.NewGuid().ToString()); // 2MB
				Console.WriteLine("[INFO] Produced 20 small messages and 1 large (2MB) message for batch test");
				await Task.Delay(2000); // Ensure messages are available

				// Batch consuming test
				// EDIT START: Changed to Exclusive for cumulative ack, set MaxNumBytes to 1MB
				var consumerData = new ConsumerData
				{
					ServiceUrl = "pulsar://localhost:6650",
					SubscriptionName = "TestSubBatch",
					TopicName = "persistent://public/default/test-topic-1",
					BatchSize = 10,
					MaxNumBytes = 1024 * 1024, // 1MB
					TimeoutMs = 10000, // 10 seconds
					SubscriptionType = SubscriptionType.Exclusive // Enable cumulative acknowledgment
				};
				// EDIT END
				var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
				var batch = await consumerManager.ProcessBatchAsync(consumerData, cts.Token);

				Console.WriteLine($"Received batch of {batch.Count} messages:");
				foreach (var msg in batch)
				{
					var displayData = msg.Data.Length > 20 ? $"{msg.Data.Substring(0, 20)}..." : msg.Data;
					Console.WriteLine($" - {displayData} (Size: {Encoding.UTF8.GetByteCount(msg.Data)} bytes, TS: {msg.UnixTimeStampMs})");
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Test Case 6 failed: {ex.Message}");
			}

			// Cleanup and keep console alive
			Console.WriteLine("\nPress any key to exit.");
			Console.ReadKey();
			if (topicManager is IAsyncDisposable asyncDisposableTopic)
			{
				await asyncDisposableTopic.DisposeAsync();
			}
			await consumerManager.DisposeAsync();

			static void PrintResponse(BaseResponse response)
			{
				Console.WriteLine(response.IsSuccess
					? $"Success: {response.Message}"
					: $"Failed: {response.Message} (Code: {response.ErrorCode})");
			}
		}
	}
}
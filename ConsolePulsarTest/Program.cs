using Microsoft.Extensions.DependencyInjection;
using SmartOps.Edge.Pulsar.BaseClasses.Contracts;
using SmartOps.Edge.Pulsar.BaseClasses.Models;
using SmartOps.Edge.Pulsar.Messages.Manager;
using DotPulsar;
using DotPulsar.Extensions;
using System.Text;

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
			serviceCollection.AddSingleton<IConsumersManager>(provider =>
				new ConsumerManager(
					null,
					SubscriptionType.Shared,
					messagePrefetchCount: 500)); // New: Set prefetch count

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
				var consumerData = new ConsumerData { SubscriptionName = "TestSubscription" };
				consumerManager.ConnectConsumer(consumerData, (c, e) => Console.WriteLine($"[ERROR] Consumer error: {e.Message}"));

				// Produce multiple messages
				await using var client = PulsarClient.Builder()
					.ServiceUrl(new Uri("pulsar://localhost:6650"))
					.Build();
				await using var producer = client.NewProducer(Schema.ByteArray)
					.Topic("persistent://public/default/test-topic-1")
					.Create();
				await producer.Send(Encoding.UTF8.GetBytes("Message 1: Success"));
				await producer.Send(Encoding.UTF8.GetBytes("Message 2: Retry"));
				await producer.Send(Encoding.UTF8.GetBytes("Message 3: Fail"));
				Console.WriteLine("[INFO] Produced 3 messages to persistent://public/default/test-topic-1");
				await Task.Delay(2000); // Ensure messages are available

				// Consume with enhanced logic
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

			// Dispose async
			if (topicManager is IAsyncDisposable asyncDisposableTopic)
			{
				await asyncDisposableTopic.DisposeAsync();
			}
			await consumerManager.DisposeAsync();

			Console.WriteLine("Press any key to exit.");
			Console.ReadKey();
		}

		static void PrintResponse(BaseResponse response)
		{
			Console.WriteLine(response.IsSuccess
				? $"Success: {response.Message}"
				: $"Failed: {response.Message} (Code: {response.ErrorCode})");
		}
	}
}
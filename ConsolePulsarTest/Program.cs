using Microsoft.Extensions.DependencyInjection;
using SmartOps.Edge.Pulsar.BaseClasses.Contracts;
using SmartOps.Edge.Pulsar.BaseClasses.Models;
using SmartOps.Edge.Pulsar.Messages.Manager;

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
			serviceCollection.AddSingleton<IProducerManager, ProducerManager>();


			var serviceProvider = serviceCollection.BuildServiceProvider();
			using var scope = serviceProvider.CreateScope();
			var topicManager = scope.ServiceProvider.GetRequiredService<ITopicManager>();

			#endregion

			// Test Case 1: Create a new topic with minimal configuration
			Console.WriteLine("Test Case 1: Create new topic with minimal config");
			var topicData1 = new CreateTopicData("pulsar://localhost:6650", $"test-topic-1")
			{
				NumPartitions = 1
			};
			var response1 = await topicManager.CreateTopic(topicData1);
			PrintResponse(response1);

			// Test Case 2: Attempt to recreate the same topic
			Console.WriteLine("\nTest Case 2: Attempt to recreate existing topic");
			var topicData2 = new CreateTopicData("pulsar://localhost:6650", $"test-topic-1")
			{
				NumPartitions = 1
			};
			var response2 = await topicManager.CreateTopic(topicData2);
			PrintResponse(response2);

			// Test Case 3: Create a topic with full configuration
			Console.WriteLine("\nTest Case 3: Create topic with full configuration");
			var topicData3 = new CreateTopicData("pulsar://localhost:6650", $"test-topic-2")
			{
				NumPartitions = 3,
				RetentionMins = "86400000", // 24 hours
				RetentionSizeMB = 1024,   // 1 GB
				ReplicationFactor = 2     // Will log a warning in standalone
			};
			var response3 = await topicManager.CreateTopic(topicData3);
			PrintResponse(response3);

			// Test Case 4: Test error handling with invalid input (negative partitions)
			Console.WriteLine("\nTest Case 4: Test error handling with invalid input (negative partitions)");
			try
			{
				var topicData4 = new CreateTopicData("pulsar://localhost:6650", $"test-topic-3")
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

			//Dispose async
			if (topicManager is IAsyncDisposable asyncDisposable)
			{
				await asyncDisposable.DisposeAsync();
			}
			// Test Case 5: Produce a message to an existing topic
			Console.WriteLine("\nTest Case 5: Produce message to 'test-topic-1'");
			var producerManager = scope.ServiceProvider.GetRequiredService<IProducerManager>();

			var producerData = new ProducerData
			{
				ServiceUrl = "pulsar://localhost:6650",
				TopicName = "test-topic-1",
				ProducerName = "console-test-producer",
				MaxPendingMessages = 1000,
				SendTimeoutMs = 30000,
				EnableBatching = true
			};

			await producerManager.ConnectProducer(producerData);

			var messageToSend = "Hello from ConsolePulsarTest!";
			await producerManager.PublishAsync("test-topic-1", messageToSend, correlationId: Guid.NewGuid().ToString());

			Console.WriteLine("\nTest Case 6: Produce a batch of messages to 'test-topic-1'");
				Console.Out.Flush();
				var batchedMessages = new List<string>
				{
					"Message 1 - from batch",
					"Message 2 - from batch",
					"Message 3 - from batch"
				};

				await producerManager.PublishBatchAsync("test-topic-1", batchedMessages, correlationId: Guid.NewGuid().ToString());
				Console.WriteLine("Debug: After PublishBatchAsync");
				Console.Out.Flush();

				Console.WriteLine("Batch publish completed.");
				Console.Out.Flush();

				int iteration = 0;
				while (iteration < 5) 
				{
					Console.WriteLine($"Producer is alive... (iteration {iteration + 1})");
					Console.Out.Flush();
					await Task.Delay(5000);
					iteration++;
				}
				if (producerManager is IDisposable disposable)
				{
					disposable.Dispose();
				}
				static void PrintResponse(BaseResponse response)
				{
					Console.WriteLine(response.IsSuccess
						? $"Success: {response.Message}"
						: $"Failed: {response.Message} (Code: {response.ErrorCode})");
				}
		}

	}
}

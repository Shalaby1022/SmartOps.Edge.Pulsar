using SmartOps.Edge.Pulsar.BaseClasses.Contracts;
using SmartOps.Edge.Pulsar.BaseClasses.Models;
using SmartOps.Edge.Pulsar.Bus.Messages.Manager;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;
using DotPulsar;
using DotPulsar.Abstractions;

namespace PulsarDemoApp
{
	public class PulsarAdminClient : IDisposable
	{
		private readonly HttpClient _httpClient;

		public PulsarAdminClient(string adminUrl)
		{
			_httpClient = new HttpClient { BaseAddress = new Uri(adminUrl) };
		}

		public async Task CreatePartitionedTopicAsync(string topic, int numPartitions)
		{
			var response = await _httpClient.PutAsync(
				$"/admin/v2/persistent/{topic}/partitions",
				new StringContent(numPartitions.ToString()));
			response.EnsureSuccessStatusCode();
		}

		public async Task<int> GetBundleCountAsync(string tenant, string namespaceName)
		{
			try
			{
				var response = await _httpClient.GetAsync($"/admin/v2/namespaces/{tenant}/{namespaceName}/bundles");
				response.EnsureSuccessStatusCode();
				var content = await response.Content.ReadAsStringAsync();
				Console.WriteLine($"[DEBUG] Bundle API response: {content}");
				var bundles = JsonSerializer.Deserialize<BundleData>(content);
				int count = bundles.Boundaries != null ? bundles.Boundaries.Count - 1 : 0;
				Console.WriteLine($"[DEBUG] Parsed bundle count: {count}");
				return count;
			}
			catch (Exception ex)
			{
				Console.WriteLine($"[ERROR] Failed to get bundle count: {ex.Message}");
				return 0;
			}
		}

		public void Dispose()
		{
			_httpClient.Dispose();
		}
	}

	public class BundleData
	{
		public List<string> Boundaries { get; set; }
		public int NumBundles { get; set; }
	}

	public class PulsarDemos
	{
		private readonly ITopicManager _topicManager;
		private readonly IProducerManager _producerManager;
		private readonly IConsumersManager _consumerManager;
		private readonly string _serviceUrl = "pulsar://localhost:6650";
		private readonly string _adminUrl = "http://localhost:8081";
		private const int TotalMessages = 10000_000;
		private const int MessageSizeBytes = 2000; // Option: Set to 500 to exceed 100 MB/s
		private const int NumberOfProducers =10 ;

		public PulsarDemos(ITopicManager topicManager, IProducerManager producerManager, IConsumersManager consumerManager)
		{
			_topicManager = topicManager ?? throw new ArgumentNullException(nameof(topicManager));
			_producerManager = producerManager ?? throw new ArgumentNullException(nameof(producerManager));
			_consumerManager = consumerManager ?? throw new ArgumentNullException(nameof(consumerManager));
		}


		public async Task RunDemo3()
		{
			var metrics = new DemoMetrics { DemoName = "Topic Hotspot Auto-Splitting" };
			var stopwatch = Stopwatch.StartNew();

			Console.WriteLine("\n=== Demo 3: Topic Hotspot Auto-Splitting (Pulsar) ===");
			Console.WriteLine("Goal: Show Pulsar's ability to automatically split hot topic bundles under high traffic.");
			Console.WriteLine($"Setup: Using a single-partition topic in public/default, sending {TotalMessages:N0} messages (~{MessageSizeBytes:N0} bytes each) in ~2-5 seconds with {NumberOfProducers} producers, repeated 12 times, no consumer acknowledgment.");
			Console.WriteLine("Config: loadBalancerNamespaceBundleMaxMsgRate=30000 msg/s, loadBalancerNamespaceBundleMaxBandwidthMbytes=100 MB/s, shedding interval=1min");
			Console.WriteLine("Verification: Run the following command in PowerShell before and after:");
			Console.WriteLine(" docker exec pulsar bin/pulsar-admin namespaces bundles public/default");
			Console.WriteLine("Expected: Bundle count increases (e.g., from 4 to 5+), indicating splitting.\n");

			IConsumersManager consumerManager = null;
			try
			{
				// Check initial bundle count
				Console.WriteLine("[CHECK] Before starting, note the initial bundle count:");
				Console.WriteLine("  docker exec pulsar bin/pulsar-admin namespaces bundles public/default");

				// Create topic with 1 partition in public/default
				var setupStopwatch = Stopwatch.StartNew();
				var topicName = $"OrderDemo3_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
				var topicData = new CreateTopicData(_serviceUrl, topicName)
				{
					Tenant = "public",
					Namespace = "default",
					NumPartitions = 1,
					RetentionMins = "10080"
				};
				Console.WriteLine($"[DEBUG] Admin URL: {_adminUrl}");
				var topicResponse = await _topicManager.CreateTopic(topicData);
				if (!topicResponse.IsSuccess)
					throw new Exception($"Topic creation failed: {topicResponse.Message}");
				Console.WriteLine($"[SETUP] {topicResponse.Message} (Took: {setupStopwatch.ElapsedMilliseconds}ms)");
				metrics.Notes += $"Topic creation: {topicResponse.Message}; ";

				// Setup consumer
				var cts = new CancellationTokenSource();
				var messageCounts = new ConcurrentDictionary<string, int>();
				var messagesReceived = new ConcurrentDictionary<string, List<string>>();
				messageCounts["Consumer-1"] = 0;
				messagesReceived["Consumer-1"] = new List<string>();

				async Task RunConsumer()
				{
					consumerManager = _consumerManager;

					var consumerData = new ConsumerData
					{
						ServiceUrl = _serviceUrl,
						TopicName = $"persistent://public/default/{topicName}-partition-0",
						SubscriptionName = "exclusive-sub",
						SubscriptionType = SubscriptionType.Exclusive,
						BatchSize = 5000
					};

					setupStopwatch.Restart();
					consumerManager.ConnectConsumer(consumerData, (c, ex) =>
						Console.WriteLine($"[ERROR] Consumer-1: {ex.Message}"));
					Console.WriteLine($"[SETUP] Consumer connected (Took: {setupStopwatch.ElapsedMilliseconds}ms)");

					Console.WriteLine("[Consumer-1] Subscribing...");
					await consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
					{
						try
						{
							if (!string.IsNullOrEmpty(msg.ExceptionMessage))
							{
								Console.WriteLine($"[Consumer-1] Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
								return;
							}
							if (msg.MessageId == null || string.IsNullOrEmpty(msg.Data))
							{
								Console.WriteLine($"[Consumer-1] Warning: Invalid message, skipping.");
								return;
							}

							if (messageCounts["Consumer-1"] < 5)
							{
								Console.ForegroundColor = ConsoleColor.Cyan;
								Console.WriteLine($"[Consumer-1] Received: {msg.Data} (MessageId: {msg.MessageId})");
								Console.ResetColor();
							}

							messagesReceived["Consumer-1"].Add(msg.Data);
							messageCounts.AddOrUpdate("Consumer-1", 1, (k, v) => v + 1);
						}
						catch (Exception ex)
						{
							Console.WriteLine($"[Consumer-1] Error processing message: {ex.Message}");
						}
					}, cts.Token);
				}

				// Start consumer
				setupStopwatch.Restart();
				var consumerTask = Task.Run(() => RunConsumer(), cts.Token);
				await Task.Delay(1000, cts.Token);
				Console.WriteLine($"[SETUP] Consumer subscribed to partition-0 (Took: {setupStopwatch.ElapsedMilliseconds}ms)");

				// Connect multiple producers
				setupStopwatch.Restart();
				var producerDatas = new List<ProducerData>();
				for (int i = 1; i <= NumberOfProducers; i++)
				{
					producerDatas.Add(new ProducerData
					{
						ServiceUrl = _serviceUrl,
						TopicName = $"persistent://public/default/{topicName}",
						ProducerName = $"producer-{i}",
						EnableBatching = true,
						BatchingMaxMessages = 50000,
						BatchingMaxDelayMs = 0
					});
				}

				foreach (var producerData in producerDatas)
				{
					await _producerManager.ConnectProducer(producerData);
				}
				Console.WriteLine($"[SETUP] {NumberOfProducers} producers connected with batching enabled (Took: {setupStopwatch.ElapsedMilliseconds}ms)");

				// Send 1,000,000 messages in ~2-5 seconds, repeat 12 times
				Console.WriteLine($"[INFO] Sending {TotalMessages:N0} messages (~{MessageSizeBytes:N0} bytes each) in ~2-5 seconds with {NumberOfProducers} producers, repeating 12 times...");
				var messageSizeEstimate = MessageSizeBytes;
				var totalMessagesSent = 0;
				var messagesPerProducer = TotalMessages / NumberOfProducers;

				for (int iteration = 1; iteration <= 12; iteration++)
				{
					Console.WriteLine($"[INFO] Starting iteration {iteration}...");
					var sendStartTime = DateTime.UtcNow;
					var producerTasks = new List<Task>();

					for (int p = 0; p < NumberOfProducers; p++)
					{
						var messages = new List<string>();
						var startIndex = totalMessagesSent + (p * messagesPerProducer) + 1;
						for (int i = 0; i < messagesPerProducer; i++)
						{
							var message = $"{{ \"eventId\": {startIndex + i}, \"type\": \"high-traffic\", \"payload\": \"{new string('a', MessageSizeBytes - 50)}\" }}";
							messages.Add(message);
						}

						var producerData = producerDatas[p];
						producerTasks.Add(Task.Run(async () =>
						{
							var batchStopwatch = Stopwatch.StartNew();
							await _producerManager.PublishBatchAsync(producerData.TopicName, messages, $"corr-{startIndex}", maxRetries: 1);
							Console.WriteLine($"[PRODUCER-{p + 1}] Sent {messages.Count:N0} messages in iteration {iteration} (Batch took: {batchStopwatch.ElapsedMilliseconds}ms)");
						}));
						metrics.MessagesSent += messages.Count;
					}

					await Task.WhenAll(producerTasks);
					var sendDuration = (DateTime.UtcNow - sendStartTime).TotalSeconds;
					var msgRate = TotalMessages / sendDuration;
					var bandwidth = (TotalMessages * messageSizeEstimate) / sendDuration / 1_000_000;
					Console.WriteLine($"[INFO] Finished iteration {iteration}: {TotalMessages:N0} messages in {sendDuration:F2}s.");
					Console.WriteLine($"[INFO] Message Rate: {msgRate:N2} msg/s (Threshold: 30000 msg/s)");
					Console.WriteLine($"[INFO] Bandwidth: {bandwidth:F2} MB/s (Threshold: 100 MB/s)");
					totalMessagesSent += TotalMessages;

					if (iteration < 12)
						await Task.Delay(5000, cts.Token);
				}

				// Wait for splitting
				Console.WriteLine("[INFO] Waiting ~2 minutes for load balancer to detect and split the hot bundle...");
				Console.WriteLine("[INFO] Load balancer checks every 60 seconds, requires threshold exceedance.");
				await Task.Delay(TimeSpan.FromMinutes(2), cts.Token);

				// Stop consumer
				Console.WriteLine("[INFO] Stopping consumer...");
				cts.Cancel();
				try
				{
					await consumerTask;
				}
				catch (OperationCanceledException)
				{
					Console.WriteLine("[INFO] Consumer stopped.");
				}

				// Check bundle count
				Console.WriteLine("\n[CHECK] To confirm bundle splitting, run:");
				Console.WriteLine("  docker exec pulsar bin/pulsar-admin namespaces bundles public/default");
				Console.WriteLine("[CHECK] Compare with initial count. An increase (e.g., 4 to 5+) confirms splitting.");
				Console.WriteLine("[TROUBLESHOOT] If no increase:");
				Console.WriteLine("  - Check logs: docker logs pulsar | Select-String 'Splitting bundle|LoadBalancer|msgRate|bandwidth'");
				Console.WriteLine("  - Verify version: docker exec pulsar bin/pulsar-admin --version");
				Console.WriteLine("  - Ensure standalone.conf has loadBalancerEnabled=true");
				Console.WriteLine("  - Test manual split: docker exec pulsar bin/pulsar-admin namespaces split-bundle public/default --bundle 0x00000000_0x40000000");
				Console.WriteLine("  - Verify namespace: docker exec pulsar bin/pulsar-admin namespaces get public/default");

				// Display results
				Console.WriteLine("\n=== Pulsar Demo 3 Results ===");
				Console.WriteLine($"Topic: {topicName} (1 partition)");
				Console.WriteLine($"Subscription: exclusive-sub");
				Console.WriteLine("Consumer   | Messages Received | Sample Messages");
				Console.WriteLine("-----------|-------------------|----------------");
				var count = messageCounts.GetValueOrDefault("Consumer-1", 0);
				var sample = messagesReceived["Consumer-1"].Take(2).Any()
					? string.Join(", ", messagesReceived["Consumer-1"].Take(2))
					: "None";
				Console.WriteLine($"Consumer-1 | {count,-17} | {sample}");

				stopwatch.Stop();
				metrics.SetupTime = stopwatch.Elapsed;
				metrics.MessagesReceived = messageCounts.Values.Sum();
				metrics.Notes += $"Sent {totalMessagesSent:N0} messages with {NumberOfProducers} producers; Received: {metrics.MessagesReceived}; Check bundle count for splitting evidence.";
				metrics.Display("Pulsar Demo 3");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"[ERROR] Pulsar Demo 3 failed: {ex.Message}");
				metrics.Notes += $"Failed: {ex.Message}";
				metrics.Display("Pulsar Demo 3");
			}
			finally
			{
				if (consumerManager != null)
					await consumerManager.DisposeAsync();

				_producerManager.Dispose();
				Console.WriteLine("[INFO] Resources disposed.");
			}
		}
		//public async Task RunDemo3()
		//{
		//	var metrics = new DemoMetrics { DemoName = "Topic Hotspot Auto-Splitting" };
		//	var stopwatch = Stopwatch.StartNew();

		//	Console.WriteLine("\n=== Demo 3: Topic Hotspot Auto-Splitting (Pulsar) ===");
		//	Console.WriteLine("Goal: Show Pulsar's ability to automatically split hot topic bundles under high traffic.");
		//	Console.WriteLine("Setup: Using a single-partition topic in public/bundleTwo, sending 3,000 messages in ~20 seconds.");
		//	Console.WriteLine("Config: loadBalancerNamespaceBundleMaxMsgRate=10 msg/s, loadBalancerNamespaceBundleMaxBandwidthMbytes=1 MB/s");
		//	Console.WriteLine("Verification: Bundle count checked programmatically before and after.");
		//	Console.WriteLine("Expected: Bundle count increases (e.g., from 4 to 5+), indicating splitting.");
		//	Console.WriteLine("Note: Ensure loadBalancerEnabled=true in standalone.conf.");
		//	Console.WriteLine("  - Check: docker exec pulsar cat conf/standalone.conf | grep loadBalancerEnabled");
		//	Console.WriteLine("  - Update: Set loadBalancerEnabled=true and restart (docker restart pulsar).");

		//	PulsarAdminClient adminClient = null;
		//	try
		//	{
		//		// Initialize admin client
		//		adminClient = new PulsarAdminClient(_adminUrl);

		//		// Check initial bundle count
		//		int initialBundleCount = await adminClient.GetBundleCountAsync("public", "bundleTwo");
		//		Console.WriteLine($"[CHECK] Initial bundle count: {initialBundleCount}");
		//		metrics.Notes += $"Initial bundle count: {initialBundleCount}; ";

		//		// Create topic with 1 partition in public/bundleTwo
		//		var topicName = $"OrderDemo3_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
		//		var topicData = new CreateTopicData(_serviceUrl, topicName)
		//		{
		//			Tenant = "public",
		//			Namespace = "bundleTwo",
		//			NumPartitions = 1,
		//			RetentionMins = "10080" // 1 week
		//		};

		//		var topicResponse = await _topicManager.CreateTopic(topicData);
		//		if (!topicResponse.IsSuccess)
		//			throw new Exception($"Topic creation failed: {topicResponse.Message}");

		//		Console.WriteLine($"[SETUP] {topicResponse.Message}");
		//		metrics.Notes += $"Topic creation: {topicResponse.Message}; ";

		//		// Setup consumer
		//		var cts = new CancellationTokenSource();
		//		var messageCounts = new ConcurrentDictionary<string, int>();
		//		var messagesReceived = new ConcurrentDictionary<string, List<string>>();
		//		messageCounts["Consumer-1"] = 0;
		//		messagesReceived["Consumer-1"] = new List<string>();

		//		async Task RunConsumer()
		//		{
		//			var consumerData = new ConsumerData
		//			{
		//				ServiceUrl = _serviceUrl,
		//				TopicName = $"persistent://public/bundleTwo/{topicName}-partition-0",
		//				SubscriptionName = "exclusive-sub",
		//				SubscriptionType = SubscriptionType.Exclusive
		//			};

		//			try
		//			{
		//				_consumerManager.ConnectConsumer(
		//					consumerData,
		//					(c, ex) => Console.WriteLine($"[ERROR] Consumer-1: {ex.Message}")
		//				);

		//				Console.WriteLine("[Consumer-1] Subscribing...");
		//				await _consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
		//				{
		//					try
		//					{
		//						var data = msg.Data;
		//						if (string.IsNullOrEmpty(data))
		//						{
		//							Console.WriteLine($"[Consumer-1] Warning: Invalid message, skipping.");
		//							return;
		//						}

		//						if (data.Contains("\"error\""))
		//						{
		//							Console.WriteLine($"[Consumer-1] Error message received: {data}");
		//							return;
		//						}

		//						Console.ForegroundColor = ConsoleColor.Cyan;
		//						Console.WriteLine($"[Consumer-1] Received: {data} (MessageId: {msg.MessageId})");
		//						Console.ResetColor();

		//						messagesReceived["Consumer-1"].Add(data);
		//						messageCounts.AddOrUpdate("Consumer-1", 1, (k, v) => v + 1);
		//					}
		//					catch (Exception ex)
		//					{
		//						Console.WriteLine($"[Consumer-1] Error processing message: {ex.Message}");
		//					}
		//				}, cts.Token);
		//			}
		//			catch (Exception ex)
		//			{
		//				Console.WriteLine($"[ERROR] Consumer setup failed: {ex.Message}");
		//				throw;
		//			}
		//		}

		//		// Start consumer
		//		var consumerTask = Task.Run(() => RunConsumer(), cts.Token);
		//		await Task.Delay(1000, cts.Token);
		//		Console.WriteLine("[SETUP] Consumer subscribed to partition-0.");

		//		// Connect producer
		//		var producerData = new ProducerData
		//		{
		//			ServiceUrl = _serviceUrl,
		//			TopicName = $"persistent://public/bundleTwo/{topicName}",
		//			EnableBatching = true,
		//			BatchingMaxMessages = 1000,
		//		};

		//		await _producerManager.ConnectProducer(producerData);
		//		Console.WriteLine("[SETUP] Producer connected.");

		//		// Send 3,000 messages in ~20 seconds to exceed 10 msg/s
		//		Console.WriteLine("[INFO] Sending 3,000 messages in ~20 seconds to exceed 10 msg/s threshold...");
		//		int messagesToSend = 50000;
		//		var sendStopwatch = Stopwatch.StartNew();

		//		for (int i = 1; i <= messagesToSend; i++)
		//		{
		//			var message = $"{{ \"eventId\": {i}, \"type\": \"high-traffic\", \"payload\": \"{new string('a', 50)}\" }}";
		//			await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
		//			metrics.MessagesSent++;

		//			if (i % 500 == 0)
		//				Console.WriteLine($"[PRODUCER] Sent {i} messages...");

		//			await Task.Delay(0, cts.Token);
		//		}

		//		sendStopwatch.Stop();
		//		Console.WriteLine($"[INFO] Finished sending {messagesToSend} messages in {sendStopwatch.Elapsed.TotalSeconds:F2} seconds.");

		//		// Wait for splitting
		//		Console.WriteLine("[INFO] Waiting ~2 minutes for load balancer to detect and split the hot bundle...");
		//		Console.WriteLine("[INFO] Load balancer checks every 60 seconds, requires 1 threshold hit.");
		//		await Task.Delay(TimeSpan.FromMinutes(2), cts.Token);

		//		// Stop consumer
		//		Console.WriteLine("[INFO] Stopping consumer...");
		//		cts.Cancel();

		//		try
		//		{
		//			await consumerTask;
		//		}
		//		catch (OperationCanceledException)
		//		{
		//			Console.WriteLine("[INFO] Consumer stopped.");
		//		}

		//		// Check final bundle count
		//		int finalBundleCount = await adminClient.GetBundleCountAsync("public", "bundleTwo");
		//		Console.WriteLine($"[CHECK] Final bundle count: {finalBundleCount}");
		//		metrics.Notes += $"Final bundle count: {finalBundleCount}; ";

		//		// Verify splitting
		//		if (finalBundleCount > initialBundleCount)
		//		{
		//			Console.WriteLine("[SUCCESS] Bundle splitting confirmed: bundle count increased!");
		//			metrics.Notes += "Bundle splitting confirmed; ";
		//		}
		//		else
		//		{
		//			Console.WriteLine("[WARNING] No bundle splitting detected. Bundle count did not increase.");
		//			metrics.Notes += "No bundle splitting detected; ";
		//			Console.WriteLine("[TROUBLESHOOT] Possible issues:");
		//			Console.WriteLine("  - Verify load balancer is enabled: docker exec pulsar bin/pulsar-admin brokers get-runtime-configurations | grep loadBalancerEnabled");
		//			Console.WriteLine("  - Check logs: docker logs pulsar | Select-String 'Splitting bundle|LoadBalancer|msgRate|bandwidth'");
		//			Console.WriteLine("  - Verify load balancer settings in standalone.conf: loadBalancerEnabled=true, loadBalancerAutoBundleSplitEnabled=true");
		//			Console.WriteLine("  - Test manual split: docker exec pulsar bin/pulsar-admin namespaces split-bundle public/bundleTwo --bundle 0x00000000_0x40000000");
		//		}

		//		// Display results
		//		Console.WriteLine("\n=== Pulsar Demo 3 Results ===");
		//		Console.WriteLine($"Topic: {topicName} (1 partition)");
		//		Console.WriteLine($"Subscription: exclusive-sub");
		//		Console.WriteLine("Consumer   | Messages Received | Sample Messages");
		//		Console.WriteLine("-----------|-------------------|----------------");

		//		var count = messageCounts.GetValueOrDefault("Consumer-1", 0);
		//		var sample = messagesReceived["Consumer-1"].Take(2).Any()
		//			? string.Join(", ", messagesReceived["Consumer-1"].Take(2))
		//			: "None";

		//		Console.WriteLine($"Consumer-1 | {count,-17} | {sample}");

		//		stopwatch.Stop();
		//		metrics.SetupTime = stopwatch.Elapsed;
		//		metrics.MessagesReceived = messageCounts.Values.Sum();
		//		metrics.Notes += $"Sent {messagesToSend} messages in {sendStopwatch.Elapsed.TotalSeconds:F2} seconds; Received: {metrics.MessagesReceived}; ";
		//		metrics.Display("Pulsar Demo 3");
		//	}
		//	catch (Exception ex)
		//	{
		//		Console.WriteLine($"[ERROR] Pulsar Demo 3 failed: {ex.Message}");
		//		metrics.Notes += $"Failed: {ex.Message}";
		//		metrics.Display("Pulsar Demo 3");
		//	}
		//	finally
		//	{
		//		adminClient?.Dispose();

		//		if (_consumerManager is IAsyncDisposable asyncConsumerDisposable)
		//			await asyncConsumerDisposable.DisposeAsync();
		//		else if (_consumerManager is IDisposable consumerDisposable)
		//			consumerDisposable.Dispose();

		//		if (_producerManager is IAsyncDisposable asyncProducerDisposable)
		//			await asyncProducerDisposable.DisposeAsync();
		//		else if (_producerManager is IDisposable producerDisposable)
		//			producerDisposable.Dispose();

		//		Console.WriteLine("[INFO] Resources disposed.");
		//	}
		//}

		//public async Task RunDemo3()
		//{
		//	var metrics = new DemoMetrics { DemoName = "Topic Hotspot Auto-Splitting" };
		//	var stopwatch = Stopwatch.StartNew();
		//	Console.WriteLine("\n=== Demo 3: Topic Hotspot Auto-Splitting (Pulsar) ===");
		//	Console.WriteLine("Goal: Demonstrate Pulsar's automatic bundle splitting under various conditions.\n");

		//	try
		//	{
		//		// Test Case 1: Splitting by Message Rate (range_equally_divide)
		//		await RunTestCase(
		//			testName: "High Message Rate",
		//			namespaceName: $"demo3_ns_msg_{Guid.NewGuid().ToString("N").Substring(0, 8)}",
		//			initialBundles: 4,
		//			splitAlgorithm: "range_equally_divide",
		//			triggerCondition: async (producerData, cts) =>
		//			{
		//				int totalMessages = 60000; // 15000 per topic, ~400 msg/s for ~150s
		//				int numTopics = 4; // One topic per bundle
		//				int messagesPerTopic = totalMessages / numTopics;
		//				int logInterval = 6000;
		//				int batchSize = 100; // Batch to reduce overhead
		//				var producers = new List<ProducerData>();
		//				var sendTasks = new List<Task>();

		//				// Create multiple topics
		//				for (int t = 0; t < numTopics; t++)
		//				{
		//					var topicName = $"HotspotDemo3_msg_{t}_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
		//					var fullTopic = $"persistent://public/{producerData.Tenant}/{topicName}";
		//					var topicData = new CreateTopicData(_serviceUrl, topicName)
		//					{
		//						Tenant = "public",
		//						Namespace = producerData.Tenant,
		//						NumPartitions = 1,
		//						RetentionMins = "10080"
		//					};

		//					var topicResponse = await _topicManager.CreateTopic(topicData);
		//					if (!topicResponse.IsSuccess)
		//						throw new Exception($"Topic creation failed: {topicResponse.Message}");

		//					var prodData = new ProducerData
		//					{
		//						ServiceUrl = _serviceUrl,
		//						Tenant = producerData.Tenant,
		//						TopicName = fullTopic,
		//						EnableBatching = false
		//					};
		//					await _producerManager.ConnectProducer(prodData);
		//					producers.Add(prodData);
		//				}

		//				// Send messages across topics
		//				for (int i = 1; i <= messagesPerTopic; i++)
		//				{
		//					var batchMessages = new List<(string Message, string CorrelationId)>();
		//					for (int t = 0; t < numTopics; t++)
		//					{
		//						var message = $"{{ \"eventId\": {i + t * messagesPerTopic}, \"data\": \"high-traffic\" }}";
		//						batchMessages.Add((message, $"corr-{i + t * messagesPerTopic}"));
		//					}

		//					foreach (var (msg, corrId) in batchMessages)
		//					{
		//						var topicIndex = (batchMessages.IndexOf((msg, corrId)) % numTopics);
		//						sendTasks.Add(_producerManager.PublishAsync(producers[topicIndex].TopicName, msg, corrId));
		//						metrics.MessagesSent++;
		//					}

		//					if (sendTasks.Count >= batchSize * numTopics || i == messagesPerTopic)
		//					{
		//						await Task.WhenAll(sendTasks);
		//						sendTasks.Clear();
		//					}

		//					if (i % logInterval == 0)
		//						Console.WriteLine($"[PRODUCER] Processing messages: {i * numTopics}/{totalMessages}");

		//					await Task.Delay(0, cts.Token); // Minimal delay to sustain load
		//				}

		//				await Task.WhenAll(sendTasks);
		//				Console.WriteLine($"[PRODUCER] Completed sending {totalMessages} messages across {numTopics} topics.");

		//				// Sustain load for splitting window
		//				Console.WriteLine("[INFO] Sustaining load for 120 seconds...");
		//				for (int i = 0; i < 120; i++)
		//				{
		//					foreach (var prod in producers)
		//					{
		//						await _producerManager.PublishAsync(prod.TopicName, "{\"eventId\": 0, \"data\": \"sustain\"}", "corr-sustain");
		//						metrics.MessagesSent++;
		//					}
		//					await Task.Delay(1000, cts.Token); // ~4 msg/s
		//				}
		//			},
		//			metrics: metrics
		//		);

		//		// Test Case 2: Splitting by Topic Count (topic_count_equally_divide)
		//		await RunTestCase(
		//			testName: "High Topic Count",
		//			namespaceName: $"demo3_ns_topic_{Guid.NewGuid().ToString("N").Substring(0, 8)}",
		//			initialBundles: 4,
		//			splitAlgorithm: "topic_count_equally_divide",
		//			triggerCondition: async (producerData, cts) =>
		//			{
		//				int totalTopics = 800; // ~200 topics per bundle
		//				for (int i = 1; i <= totalTopics; i++)
		//				{
		//					var topicName = $"HotspotDemo3_subtopic_{i:D4}_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
		//					var fullTopic = $"persistent://public/{producerData.Tenant}/{topicName}";
		//					var topicData = new CreateTopicData(_serviceUrl, topicName)
		//					{
		//						Tenant = "public",
		//						Namespace = producerData.Tenant,
		//						NumPartitions = 1,
		//						RetentionMins = "10080"
		//					};

		//					var topicResponse = await _topicManager.CreateTopic(topicData);
		//					if (!topicResponse.IsSuccess)
		//						throw new Exception($"Subtopic creation failed: {topicResponse.Message}");

		//					var message = $"{{ \"topicId\": {i}, \"data\": \"subtopic\" }}";
		//					await _producerManager.PublishAsync(fullTopic, message, $"corr-{i}");
		//					metrics.MessagesSent++;

		//					if (i % 10 == 0)
		//						Console.WriteLine($"[PRODUCER] Created and sent to topic {i}/{totalTopics}: {fullTopic}");

		//					await Task.Delay(5, cts.Token); // ~200 topics/s, ~4s total
		//				}
		//				Console.WriteLine($"[PRODUCER] Completed creating {totalTopics} subtopics.");
		//			},
		//			metrics: metrics
		//		);

		//		// Test Case 3: Manual Splitting (specified_positions_divide)
		//		await RunTestCase(
		//			testName: "Manual Splitting",
		//			namespaceName: $"demo3_ns_manual_{Guid.NewGuid().ToString("N").Substring(0, 8)}",
		//			initialBundles: 4,
		//			splitAlgorithm: "specified_positions_divide",
		//			triggerCondition: async (producerData, cts) =>
		//			{
		//				int totalMessages = 10000; // Robust load
		//				int logInterval = 1000;
		//				int batchSize = 100;
		//				var batchMessages = new List<(string Message, string CorrelationId)>();

		//				for (int i = 1; i <= totalMessages; i++)
		//				{
		//					var message = $"{{ \"eventId\": {i}, \"data\": \"manual-split\" }}";
		//					batchMessages.Add((message, $"corr-{i}"));
		//					metrics.MessagesSent++;

		//					if (batchMessages.Count >= batchSize || i == totalMessages)
		//					{
		//						var sendTasks = batchMessages.Select(m => _producerManager.PublishAsync(producerData.TopicName, m.Message, m.CorrelationId)).ToList();
		//						await Task.WhenAll(sendTasks);
		//						batchMessages.Clear();
		//					}

		//					if (i % logInterval == 0)
		//						Console.WriteLine($"[PRODUCER] Processing messages: {i}/{totalMessages}");

		//					await Task.Delay(0, cts.Token); // Minimal delay
		//				}
		//				Console.WriteLine($"[PRODUCER] Completed sending {totalMessages} messages.");

		//				var bundleRange = await GetBundleRange(producerData.Tenant);
		//				await TriggerManualSplit(producerData.Tenant, bundleRange, "0x80000000");
		//			},
		//			metrics: metrics
		//		);

		//		stopwatch.Stop();
		//		metrics.SetupTime = stopwatch.Elapsed;
		//		metrics.MessagesReceived = 0;
		//		metrics.Display("Pulsar Demo 3");

		//		Console.WriteLine("\n=== Kafka Comparison ===");
		//		Console.WriteLine("Kafka: Partitions are static and require manual repartitioning to handle hotspots.");
		//		Console.WriteLine("       This involves creating a new topic with more partitions and migrating data, which is disruptive.");
		//	}
		//	catch (Exception ex)
		//	{
		//		Console.WriteLine($"[ERROR] Pulsar Demo 3 failed: {ex.Message}");
		//		metrics.Notes += $"Failed: {ex.Message}";
		//		metrics.Display("Pulsar Demo 3");
		//	}
		//	finally
		//	{
		//		_producerManager.Dispose();
		//		Console.WriteLine("[INFO] Resources disposed.");
		//	}
		//}

		//private async Task RunTestCase(string testName, string namespaceName, int initialBundles, string splitAlgorithm,
		//	Func<ProducerData, CancellationTokenSource, Task> triggerCondition, DemoMetrics metrics)
		//{
		//	Console.WriteLine($"\n=== Test Case: {testName} ===");
		//	var cts = new CancellationTokenSource();
		//	var testMetrics = new Dictionary<string, string>();
		//	var stopwatch = Stopwatch.StartNew();

		//	try
		//	{
		//		await CreateNamespace(namespaceName, initialBundles, splitAlgorithm);
		//		testMetrics["Namespace"] = namespaceName;

		//		var topicName = $"HotspotDemo3_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
		//		var topicData = new CreateTopicData(_serviceUrl, topicName)
		//		{
		//			Tenant = "public",
		//			Namespace = namespaceName,
		//			NumPartitions = 1,
		//			RetentionMins = "10080"
		//		};

		//		var topicResponse = await _topicManager.CreateTopic(topicData);
		//		if (!topicResponse.IsSuccess)
		//			throw new Exception($"Topic creation failed: {topicResponse.Message}");

		//		Console.WriteLine($"[SETUP] {topicResponse.Message}");
		//		testMetrics["Topic"] = topicName;

		//		var producerData = new ProducerData
		//		{
		//			ServiceUrl = _serviceUrl,
		//			Tenant = namespaceName,
		//			TopicName = $"persistent://public/{namespaceName}/{topicName}",
		//			EnableBatching = false
		//		};

		//		await _producerManager.ConnectProducer(producerData);
		//		Console.WriteLine("[SETUP] Producer connected.");

		//		var initialBundleCount = await GetBundleCount(namespaceName);
		//		Console.WriteLine($"[INFO] Initial number of bundles: {initialBundleCount}");
		//		testMetrics["InitialBundles"] = initialBundleCount.ToString();

		//		if (initialBundleCount != initialBundles)
		//			throw new Exception($"Expected {initialBundles} initial bundle(s), but found {initialBundleCount}. Ensure broker settings align with 'defaultNumberOfNamespaceBundles=4'.");

		//		Console.WriteLine($"[INFO] Triggering {testName} condition...");
		//		var startTime = DateTime.UtcNow;
		//		await triggerCondition(producerData, cts);
		//		var endTime = DateTime.UtcNow;
		//		var durationSeconds = (endTime - startTime).TotalSeconds;
		//		var msgRate = durationSeconds > 0 ? metrics.MessagesSent / durationSeconds : 0;
		//		Console.WriteLine($"[INFO] Message rate: {msgRate:F2} msg/s");

		//		Console.WriteLine("[INFO] Waiting for bundle splitting to occur...");
		//		var maxWaitSeconds = 240; // Wait up to 4 minutes
		//		var finalBundleCount = initialBundleCount;
		//		var unchangedSeconds = 0;

		//		for (int i = 0; i < maxWaitSeconds; i++)
		//		{
		//			var currentBundleCount = await GetBundleCount(namespaceName);
		//			if (currentBundleCount > initialBundleCount)
		//			{
		//				finalBundleCount = currentBundleCount;
		//				Console.WriteLine($"[SUCCESS] Bundle splitting detected! Number of bundles increased to: {finalBundleCount}");
		//				break;
		//			}
		//			else if (currentBundleCount == initialBundleCount)
		//			{
		//				unchangedSeconds++;
		//				if (unchangedSeconds % 30 == 0)
		//				{
		//					Console.WriteLine($"[INFO] No bundle splitting detected after {unchangedSeconds} seconds.");
		//					var stats = await GetBundleStats(namespaceName);
		//					Console.WriteLine($"[DEBUG] Bundle stats: {stats}");
		//				}
		//			}

		//			await Task.Delay(1000, cts.Token);
		//		}

		//		stopwatch.Stop();
		//		testMetrics["TestDuration"] = stopwatch.Elapsed.TotalSeconds.ToString("F2") + " seconds";

		//		Console.WriteLine($"\n=== {testName} Results ===");
		//		Console.WriteLine($"Namespace: {namespaceName}");
		//		Console.WriteLine($"Topic: {topicName}");
		//		Console.WriteLine($"Split Algorithm: {splitAlgorithm}");
		//		Console.WriteLine($"Initial Bundles: {initialBundleCount}");
		//		Console.WriteLine($"Final Bundles: {finalBundleCount}");
		//		Console.WriteLine($"Messages Sent: {metrics.MessagesSent}");
		//		Console.WriteLine($"Message Rate: {msgRate:F2} msg/s");
		//		Console.WriteLine($"Bundle Splitting {(finalBundleCount > initialBundleCount ? "Occurred" : "Did Not Occur")}");

		//		testMetrics["FinalBundles"] = finalBundleCount.ToString();
		//		testMetrics["SplittingOccurred"] = finalBundleCount > initialBundleCount ? "Yes" : "No";
		//		metrics.Notes += $"{testName}: " + string.Join("; ", testMetrics.Select(kv => $"{kv.Key}: {kv.Value}")) + "; ";
		//	}
		//	catch (Exception ex)
		//	{
		//		Console.WriteLine($"[ERROR] {testName} failed: {ex.Message}");
		//		metrics.Notes += $"{testName} failed: {ex.Message}; ";
		//	}
		//	finally
		//	{
		//		cts.Cancel();
		//		stopwatch.Stop();
		//	}
		//}

		//private async Task CreateNamespace(string namespaceName, int bundles, string splitAlgorithm)
		//{
		//	using var client = new HttpClient();
		//	var url = $"{_adminUrl}/admin/v2/namespaces/public/{namespaceName}";
		//	var stopwatch = Stopwatch.StartNew();

		//	// Step 1: Delete existing namespace
		//	try
		//	{
		//		var deleteResponse = await client.DeleteAsync(url);
		//		Console.WriteLine($"[SETUP] Delete existing namespace {namespaceName}: {(deleteResponse.IsSuccessStatusCode ? "Success" : $"Failed - {deleteResponse.StatusCode}")}");
		//	}
		//	catch (Exception ex)
		//	{
		//		Console.WriteLine($"[SETUP] Failed to delete existing namespace {namespaceName}: {ex.Message}");
		//	}

		//	// Step 2: Create namespace with 4 bundles
		//	var boundaries = new List<string> { "0x00000000" };
		//	for (int i = 1; i < bundles; i++)
		//	{
		//		boundaries.Add($"0x{i * 0xffffffff / bundles:x8}");
		//	}
		//	boundaries.Add("0xffffffff"); // Ensure end boundary
		//	var payload = new
		//	{
		//		clusters = new[] { "standalone" },
		//		policies = new
		//		{
		//			bundle_config = new
		//			{
		//				boundaries = boundaries.ToArray(),
		//				num_bundles = bundles
		//			},
		//			namespace_bundle_split_algorithm = splitAlgorithm,
		//			auto_bundle_split_enabled = true // Explicitly enable
		//		}
		//	};

		//	var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
		//	int maxRetries = 5;
		//	for (int retry = 0; retry < maxRetries; retry++)
		//	{
		//		try
		//		{
		//			var response = await client.PutAsync(url, content);
		//			if (response.IsSuccessStatusCode)
		//			{
		//				Console.WriteLine($"[SETUP] Namespace creation took {stopwatch.Elapsed.TotalSeconds:F2} seconds.");
		//				break;
		//			}
		//			var errorContent = await response.Content.ReadAsStringAsync();
		//			Console.WriteLine($"[WARN] Failed to create namespace (attempt {retry + 1}/{maxRetries}): {response.StatusCode} - {errorContent}");
		//			await Task.Delay(2000);
		//		}
		//		catch (Exception ex)
		//		{
		//			Console.WriteLine($"[WARN] Namespace creation error (attempt {retry + 1}/{maxRetries}): {ex.Message}");
		//			await Task.Delay(2000);
		//		}
		//	}

		//	// Step 3: Verify bundle count
		//	int retries = 5;
		//	int actualBundleCount = 0;
		//	for (int i = 0; i < retries; i++)
		//	{
		//		actualBundleCount = await GetBundleCount(namespaceName);
		//		if (actualBundleCount == bundles)
		//			break;

		//		Console.WriteLine($"[WARN] Expected {bundles} bundle(s), but found {actualBundleCount}. Retrying in 1 second...");
		//		await Task.Delay(1000);
		//	}

		//	if (actualBundleCount != bundles)
		//		throw new Exception($"Namespace {namespaceName} created with {actualBundleCount} bundle(s), expected {bundles}. Ensure broker settings align with 'defaultNumberOfNamespaceBundles=4'.");

		//	Console.WriteLine($"[SETUP] Namespace {namespaceName} created with {bundles} bundle(s) and {splitAlgorithm}.");
		//}

		//private async Task<int> GetBundleCount(string namespaceName)
		//{
		//	using var client = new HttpClient();
		//	var url = $"{_adminUrl}/admin/v2/namespaces/public/{namespaceName}/bundles";
		//	try
		//	{
		//		var response = await client.GetAsync(url);
		//		if (!response.IsSuccessStatusCode)
		//			throw new Exception($"Failed to get bundle count: {response.StatusCode}");

		//		var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
		//		var boundaries = json.RootElement.GetProperty("boundaries").EnumerateArray().ToList();
		//		Console.WriteLine($"[DEBUG] Bundle boundaries for {namespaceName}: [{string.Join(", ", boundaries.Select(b => b.GetString()))}]");
		//		return boundaries.Count - 1; // Number of bundles = boundaries.Length - 1
		//	}
		//	catch (Exception ex)
		//	{
		//		Console.WriteLine($"[ERROR] Failed to get bundle count: {ex.Message}");
		//		return -1;
		//	}
		//}

		//private async Task<string> GetBundleRange(string namespaceName)
		//{
		//	using var client = new HttpClient();
		//	var url = $"{_adminUrl}/admin/v2/namespaces/public/{namespaceName}/bundles";
		//	var response = await client.GetAsync(url);

		//	if (!response.IsSuccessStatusCode)
		//		throw new Exception($"Failed to get bundle range: {response.StatusCode} - {await response.Content.ReadAsStringAsync()}");

		//	var content = await response.Content.ReadAsStringAsync();
		//	var bundleData = JsonSerializer.Deserialize<Dictionary<string, object>>(content);
		//	var boundaries = JsonSerializer.Deserialize<List<string>>(bundleData["boundaries"].ToString());
		//	if (boundaries != null && boundaries.Count >= 2)
		//	{
		//		return $"{boundaries[0]}_{boundaries[1]}";
		//	}
		//	return string.Join(",", boundaries);
		//}

		//private async Task<string> GetBundleStats(string namespaceName)
		//{
		//	using var client = new HttpClient();
		//	var url = $"{_adminUrl}/admin/v2/namespaces/public/{namespaceName}/stats";
		//	try
		//	{
		//		var response = await client.GetAsync(url);
		//		if (!response.IsSuccessStatusCode)
		//			return $"Failed to get bundle stats: {response.StatusCode}";

		//		var content = await response.Content.ReadAsStringAsync();
		//		var stats = JsonSerializer.Deserialize<Dictionary<string, object>>(content);
		//		var summary = new List<string>();
		//		if (stats.ContainsKey("bundles"))
		//		{
		//			var bundles = JsonSerializer.Deserialize<Dictionary<string, Dictionary<string, object>>>(stats["bundles"].ToString());
		//			foreach (var bundle in bundles)
		//			{
		//				var msgRateIn = bundle.Value.ContainsKey("msgRateIn") ? bundle.Value["msgRateIn"].ToString() : "N/A";
		//				var topics = bundle.Value.ContainsKey("topics") ? bundle.Value["topics"].ToString() : "N/A";
		//				summary.Add($"{bundle.Key}: msgRateIn={msgRateIn}, topics={topics}");
		//			}
		//		}
		//		return summary.Any() ? string.Join("; ", summary) : "No bundle stats available";
		//	}
		//	catch (Exception ex)
		//	{
		//		return $"Failed to get bundle stats: {ex.Message}";
		//	}
		//}

		//private async Task TriggerManualSplit(string namespaceName, string bundleRange, string splitPosition)
		//{
		//	using var client = new HttpClient();
		//	var url = $"{_adminUrl}/admin/v2/namespaces/public/{namespaceName}/splitBundle?splitAlgorithmName=specified_positions_divide";
		//	var payload = new
		//	{
		//		bundle = bundleRange,
		//		splitPositions = new[] { splitPosition }
		//	};
		//	var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");

		//	var response = await client.PostAsync(url, content);
		//	if (!response.IsSuccessStatusCode)
		//	{
		//		var errorContent = await response.Content.ReadAsStringAsync();
		//		throw new Exception($"Failed to trigger manual split: {response.StatusCode} - {errorContent}");
		//	}

		//	Console.WriteLine($"[INFO] Manual split triggered successfully at position {splitPosition} for bundle {bundleRange}.");
		//}
		//public async Task RunDemo3()
		//		{
		//			var metrics = new DemoMetrics { DemoName = "Topic Hotspot Auto-Splitting" };
		//			var stopwatch = Stopwatch.StartNew();

		//			Console.WriteLine("\n=== Demo 3: Topic Hotspot Auto-Splitting (Pulsar) ===");
		//			Console.WriteLine("Goal: Show Pulsar's ability to automatically split hot topic bundles under high traffic.");
		//			Console.WriteLine("Setup: Using a single-partition topic in public/bundleTwo, sending 50,000 messages over ~300 seconds.");
		//			Console.WriteLine("Config: loadBalancerNamespaceBundleMaxMsgRate=0.5, loadBalancerNamespaceBundleMaxBandwidthMbytes=0.0005, split interval=0.5min");
		//			Console.WriteLine("Verification: Run the following command in PowerShell before and after:");
		//			Console.WriteLine("  docker exec pulsar bin/pulsar-admin namespaces bundles public/bundleTwo");
		//			Console.WriteLine("Expected: Bundle count increases (e.g., from 4 to 5+), indicating splitting.\n");

		//			ConsumerManager consumerManager = null;
		//			try
		//			{
		//				// Check initial bundle count
		//				Console.WriteLine("[CHECK] Before starting, note the initial bundle count:");
		//				Console.WriteLine("  docker exec pulsar bin/pulsar-admin namespaces bundles public/bundleTwo");

		//				// Create topic with 1 partition in public/bundleTwo
		//				var topicName = $"OrderDemo3_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
		//				var topicData = new CreateTopicData(_serviceUrl, topicName)
		//				{
		//					Tenant = "public",
		//					Namespace = "bundleTwo",
		//					NumPartitions = 1, // Single partition to satisfy constraint
		//					RetentionMins = "10080" // 1 week
		//				};

		//				var topicResponse = await _topicManager.CreateTopic(topicData);
		//				if (!topicResponse.IsSuccess)
		//					throw new Exception($"Topic creation failed: {topicResponse.Message}");

		//				Console.WriteLine($"[SETUP] {topicResponse.Message}");
		//				metrics.Notes += $"Topic creation: {topicResponse.Message}; ";

		//				// Setup consumer
		//				var cts = new CancellationTokenSource();
		//				var messageCounts = new ConcurrentDictionary<string, int>();
		//				var messagesReceived = new ConcurrentDictionary<string, List<string>>();
		//				messageCounts["Consumer-1"] = 0;
		//				messagesReceived["Consumer-1"] = new List<string>();

		//				async Task RunConsumer()
		//				{
		//					consumerManager = new ConsumerManager(
		//						errorHandler: (c, ex) => Console.WriteLine($"[ERROR] Consumer-1: {ex.Message}"),
		//						subscriptionType: SubscriptionType.Exclusive,
		//						messagePrefetchCount: 100
		//					);

		//					var consumerData = new ConsumerData
		//					{
		//						ServiceUrl = _serviceUrl,
		//						TopicName = $"persistent://public/bundleTwo/{topicName}-partition-0",
		//						SubscriptionName = "exclusive-sub",
		//						SubscriptionType = SubscriptionType.Exclusive
		//					};

		//					consumerManager.ConnectConsumer(
		//						consumerData,
		//						(c, ex) => Console.WriteLine($"[ERROR] Consumer-1: {ex.Message}")
		//					);

		//					Console.WriteLine("[Consumer-1] Subscribing...");
		//					await consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
		//					{
		//						try
		//						{
		//							if (!string.IsNullOrEmpty(msg.ExceptionMessage))
		//							{
		//								Console.WriteLine($"[Consumer-1] Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
		//								return;
		//							}

		//							if (msg.MessageId == null || string.IsNullOrEmpty(msg.Data))
		//							{
		//								Console.WriteLine($"[Consumer-1] Warning: Invalid message, skipping.");
		//								return;
		//							}

		//							Console.ForegroundColor = ConsoleColor.Cyan;
		//							Console.WriteLine($"[Consumer-1] Received: {msg.Data} (MessageId: {msg.MessageId})");
		//							Console.ResetColor();

		//							messagesReceived["Consumer-1"].Add(msg.Data);
		//							messageCounts.AddOrUpdate("Consumer-1", 1, (k, v) => v + 1);

		//							await msg.Consumer.Acknowledge(msg.MessageId);
		//						}
		//						catch (Exception ex)
		//						{
		//							Console.WriteLine($"[Consumer-1] Error processing message: {ex.Message}");
		//						}
		//					}, cts.Token);
		//				}

		//				// Start consumer
		//				var consumerTask = Task.Run(() => RunConsumer(), cts.Token);
		//				await Task.Delay(1000, cts.Token);
		//				Console.WriteLine("[SETUP] Consumer subscribed to partition-0.");

		//				// Connect producer
		//				var producerData = new ProducerData
		//				{
		//					ServiceUrl = _serviceUrl,
		//					TopicName = $"persistent://public/bundleTwo/{topicName}",
		//					EnableBatching = false
		//				};

		//				await _producerManager.ConnectProducer(producerData);
		//				Console.WriteLine("[SETUP] Producer connected.");

		//				// Send 50,000 messages over ~300 seconds
		//				Console.WriteLine("[INFO] Sending 50,000 messages over ~300 seconds to exceed 0.5 msg/s and 0.0005 MB/s thresholds...");
		//				var messageSizeEstimate = 100; // Approx bytes per message

		//				for (int i = 1; i <= 50000; i++)
		//				{
		//					var message = $"{{ \"eventId\": {i}, \"type\": \"high-traffic\", \"payload\": \"{new string('a', 50)}\" }}";
		//					await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
		//					metrics.MessagesSent++;

		//					if (i % 5000 == 0)
		//						Console.WriteLine($"[PRODUCER] Sent {i} messages...");

		//					await Task.Delay(6, cts.Token); // ~300 seconds total
		//				}

		//				Console.WriteLine("[INFO] Finished sending 50,000 messages.");

		//				// Wait for splitting
		//				Console.WriteLine("[INFO] Waiting ~5 minutes for load balancer to detect and split the hot bundle...");
		//				Console.WriteLine("[INFO] Load balancer checks every 30 seconds, requires 1 threshold hit.");
		//				await Task.Delay(TimeSpan.FromMinutes(5), cts.Token);

		//				// Stop consumer
		//				Console.WriteLine("[INFO] Stopping consumer...");
		//				cts.Cancel();

		//				try
		//				{
		//					await consumerTask;
		//				}
		//				catch (OperationCanceledException)
		//				{
		//					Console.WriteLine("[INFO] Consumer stopped.");
		//				}

		//				// Check bundle count
		//				Console.WriteLine("\n[CHECK] To confirm bundle splitting, run:");
		//				Console.WriteLine("  docker exec pulsar bin/pulsar-admin namespaces bundles public/bundleTwo");
		//				Console.WriteLine("[CHECK] Compare with initial count. An increase (e.g., 4 to 5+) confirms splitting.");
		//				Console.WriteLine("[TROUBLESHOOT] If no increase:");
		//				Console.WriteLine("  - Check logs: docker logs pulsar | Select-String 'Splitting bundle|LoadBalancer|msgRate|bandwidth'");
		//				Console.WriteLine("  - Verify version: docker exec pulsar bin/pulsar-admin --version");
		//				Console.WriteLine("  - Ensure broker.conf has loadBalancerAutoBundleSplitEnabled=true, thresholds 0.5 msg/s, 0.0005 MB/s");
		//				Console.WriteLine("  - Test manual split: docker exec pulsar bin/pulsar-admin namespaces split-bundle public/bundleTwo --bundle 0x00000000_0x40000000");
		//				Console.WriteLine("  - Reset namespace: docker exec pulsar bin/pulsar-admin namespaces delete public/bundleTwo --force; docker exec pulsar bin/pulsar-admin namespaces create public/bundleTwo");

		//				// Display results
		//				Console.WriteLine("\n=== Pulsar Demo 3 Results ===");
		//				Console.WriteLine($"Topic: {topicName} (1 partition)");
		//				Console.WriteLine($"Subscription: exclusive-sub");
		//				Console.WriteLine("Consumer   | Messages Received | Sample Messages");
		//				Console.WriteLine("-----------|-------------------|----------------");

		//				var count = messageCounts.GetValueOrDefault("Consumer-1", 0);
		//				var sample = messagesReceived["Consumer-1"].Take(2).Any()
		//					? string.Join(", ", messagesReceived["Consumer-1"].Take(2))
		//					: "None";

		//				Console.WriteLine($"Consumer-1 | {count,-17} | {sample}");

		//				stopwatch.Stop();
		//				metrics.SetupTime = stopwatch.Elapsed;
		//				metrics.MessagesReceived = messageCounts.Values.Sum();
		//				metrics.Notes += $"Sent 50,000 messages; Received: {metrics.MessagesReceived}; Check bundle count for splitting evidence.";
		//				metrics.Display("Pulsar Demo 3");
		//			}
		//			catch (Exception ex)
		//			{
		//				Console.WriteLine($"[ERROR] Pulsar Demo 3 failed: {ex.Message}");
		//				metrics.Notes += $"Failed: {ex.Message}";
		//				metrics.Display("Pulsar Demo 3");
		//			}
		//			finally
		//			{
		//				if (consumerManager != null)
		//					await consumerManager.DisposeAsync();

		//				_producerManager.Dispose();
		//				Console.WriteLine("[INFO] Resources disposed.");
		//			}
		//		}

		public async Task RunDemo1()
		{
			var metrics = new DemoMetrics { DemoName = "Multiple Subscriptions on the Same Topic" };
			var stopwatch = Stopwatch.StartNew();
			Console.WriteLine("\n=== Demo 1: Multiple Subscriptions on the Same Topic (Pulsar) ===");
			Console.WriteLine("Goal: Show Pulsar's ability to have multiple independent subscriptions on one topic.\n");

			try
			{
				// Create topic
				var topicName = $"OrderDemo1_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
				var topicData = new CreateTopicData(_serviceUrl, topicName)
				{
					Tenant = "public",
					Namespace = "default",
					NumPartitions = 1,
					RetentionMins = "10080" // 1 week for demo
				};
				var topicResponse = await _topicManager.CreateTopic(topicData);
				Console.WriteLine($"[SETUP] {topicResponse.Message}");
				metrics.Notes += $"Topic creation: {topicResponse.Message}; ";

				// Define subscriptions
				var subscriptions = new[] { "analytics-sub", "fraud-sub", "billing-sub" };
				var cts = new CancellationTokenSource();
				var messageCounts = new ConcurrentDictionary<string, int>();
				var messagesReceived = new ConcurrentDictionary<string, List<string>>();

				foreach (var sub in subscriptions)
				{
					messageCounts[sub] = 0;
					messagesReceived[sub] = new List<string>();
				}

				// Consumer logic
				async Task RunConsumer(string subName)
				{
					var consumerData = new ConsumerData
					{
						ServiceUrl = _serviceUrl,
						TopicName = $"persistent://public/default/{topicName}",
						SubscriptionName = subName,
						SubscriptionType = SubscriptionType.Exclusive
					};

					var isolatedConsumerManager = new ConsumerManager();
					isolatedConsumerManager.ConnectConsumer(consumerData, (c, ex) =>
						Console.WriteLine($"[ERROR] Consumer {subName}: {ex.Message}"));

					Console.WriteLine($"[{subName}] Subscribing...");
					await isolatedConsumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
					{
						try
						{
							if (!string.IsNullOrEmpty(msg.ExceptionMessage))
							{
								Console.WriteLine($"[{subName}] Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
								return;
							}
							if (msg.MessageId == null || string.IsNullOrEmpty(msg.Data))
							{
								Console.WriteLine($"[{subName}] Warning: Invalid message, skipping.");
								return;
							}

							Console.ForegroundColor = subName switch
							{
								"analytics-sub" => ConsoleColor.Cyan,
								"fraud-sub" => ConsoleColor.Magenta,
								"billing-sub" => ConsoleColor.Yellow,
								_ => ConsoleColor.White
							};
							Console.WriteLine($"[{subName}] Received: {msg.Data} (MessageId: {msg.MessageId})");
							Console.ResetColor();

							messagesReceived[subName].Add(msg.Data);
							var count = messageCounts.AddOrUpdate(subName, 1, (_, c) => c + 1);

							await msg.Consumer.Acknowledge(msg.MessageId);

							if (count >= 10)
							{
								Console.WriteLine($"[{subName}] Received 10 messages, ready to complete.");
								if (messageCounts.All(kv => kv.Value >= 10))
								{
									cts.Cancel();
								}
							}
						}
						catch (Exception ex)
						{
							Console.WriteLine($"[{subName}] Error processing message: {ex.Message}");
						}
					}, cts.Token);
				}

				// Start consumers
				var consumerTasks = subscriptions.Select(sub => Task.Run(() => RunConsumer(sub), cts.Token)).ToArray();
				await Task.Delay(1000, cts.Token); // Allow consumers to subscribe

				// Publish messages
				var producerData = new ProducerData
				{
					ServiceUrl = _serviceUrl,
					TopicName = $"persistent://public/default/{topicName}",
					EnableBatching = false
				};
				await _producerManager.ConnectProducer(producerData);
				Console.WriteLine("[SETUP] Producer connected.");

				for (int i = 1; i <= 10; i++)
				{
					var message = $"{{ \"orderId\": {i}, \"amount\": {i * 100} }}";
					await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
					metrics.MessagesSent++;
					Console.WriteLine($"[PRODUCER] Sent message {i}: {message}");
					await Task.Delay(100, cts.Token);
				}

				// Wait for consumers
				try
				{
					await Task.WhenAll(consumerTasks);
				}
				catch (OperationCanceledException)
				{
					Console.WriteLine("[INFO] Consumers completed processing.");
				}

				// Display results
				Console.WriteLine("\n=== Pulsar Demo 1 Results ===");
				Console.WriteLine($"Topic: {topicName}");
				Console.WriteLine("Subscription | Messages Received | Sample Messages");
				Console.WriteLine("-------------|-------------------|----------------");
				foreach (var sub in subscriptions)
				{
					var count = messageCounts.GetValueOrDefault(sub, 0);
					var sample = messagesReceived[sub].Take(2).Any() ? string.Join(", ", messagesReceived[sub].Take(2)) : "None";
					Console.WriteLine($"{sub,-12} | {count,-17} | {sample}");
				}

				stopwatch.Stop();
				metrics.SetupTime = stopwatch.Elapsed;
				metrics.MessagesReceived = messageCounts.Values.Sum();
				metrics.Notes += $"Total messages received: {metrics.MessagesReceived}; " +
								 string.Join("; ", messageCounts.Select(kv => $"{kv.Key}: {kv.Value} messages"));
				metrics.Display("Pulsar Demo 1");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"[ERROR] Pulsar Demo 1 failed: {ex.Message}");
				metrics.Notes += $"Failed: {ex.Message}";
				metrics.Display("Pulsar Demo 1");
			}
			finally
			{
				await _consumerManager.DisposeAsync();
				_producerManager.Dispose();
			}
		}
		public async Task RunDemo2()
		{
			var metrics = new DemoMetrics { DemoName = "Shared Subscription for Parallel Processing" };
			var stopwatch = Stopwatch.StartNew();
			Console.WriteLine("\n=== Demo 2: Shared Subscription for Parallel Processing (Pulsar) ===");
			Console.WriteLine("Goal: Show Pulsar's ability to distribute messages across multiple consumers in a Shared subscription.\n");

			List<ConsumerManager> consumerManagers = new();
			try
			{
				// Create topic
				var topicName = $"OrderDemo2_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
				var topicData = new CreateTopicData(_serviceUrl, topicName)
				{
					Tenant = "public",
					Namespace = "default",
					NumPartitions = 1,
					RetentionMins = "10080" // 1 week
				};
				var topicResponse = await _topicManager.CreateTopic(topicData);
				if (!topicResponse.IsSuccess)
				{
					throw new Exception($"Topic creation failed: {topicResponse.Message}");
				}
				Console.WriteLine($"[SETUP] {topicResponse.Message}");
				metrics.Notes += $"Topic creation: {topicResponse.Message}; ";

				// Prepare
				var cts = new CancellationTokenSource();
				var messageCounts = new ConcurrentDictionary<int, int>();
				var messagesReceived = new ConcurrentDictionary<int, List<string>>();
				var readySignals = new Dictionary<int, TaskCompletionSource<bool>>();
				var pendingAcks = new ConcurrentDictionary<int, TaskCompletionSource<bool>>();
				var cancellationLock = new object();
				bool cancellationTriggered = false;

				for (int i = 1; i <= 3; i++)
				{
					messageCounts[i] = 0;
					messagesReceived[i] = new List<string>();
					readySignals[i] = new TaskCompletionSource<bool>();
					pendingAcks[i] = new TaskCompletionSource<bool>();
				}

				// Consumer logic
				async Task RunConsumer(int consumerId)
				{
					var consumerManager = new ConsumerManager(
						errorHandler: (c, ex) => Console.WriteLine($"[ERROR] Consumer-{consumerId}: {ex.Message}"),
						subscriptionType: SubscriptionType.Shared,
						messagePrefetchCount: 2 // Increased for stability
					);
					consumerManagers.Add(consumerManager);

					var consumerData = new ConsumerData
					{
						ServiceUrl = _serviceUrl,
						TopicName = $"persistent://public/default/{topicName}",
						SubscriptionName = "shared-sub",
						SubscriptionType = SubscriptionType.Shared
					};

					try
					{
						consumerManager.ConnectConsumer(consumerData, (c, ex) =>
							Console.WriteLine($"[ERROR] Consumer-{consumerId}: {ex.Message}"));

						Console.WriteLine($"[Consumer-{consumerId}] Subscribing to shared-sub...");
						readySignals[consumerId].SetResult(true);

						await consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
						{
							try
							{
								if (!string.IsNullOrEmpty(msg.ExceptionMessage))
								{
									Console.WriteLine($"[Consumer-{consumerId}] Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
									return;
								}
								if (msg.MessageId == null || string.IsNullOrEmpty(msg.Data))
								{
									Console.WriteLine($"[Consumer-{consumerId}] Warning: Invalid message, skipping.");
									return;
								}

								if (messageCounts.Values.Sum() >= 30)
								{
									return;
								}

								Console.ForegroundColor = consumerId switch
								{
									1 => ConsoleColor.Cyan,
									2 => ConsoleColor.Magenta,
									3 => ConsoleColor.Yellow,
									_ => ConsoleColor.White
								};
								Console.WriteLine($"[Consumer-{consumerId}] Received: {msg.Data} (MessageId: {msg.MessageId})");
								Console.ResetColor();

								messagesReceived[consumerId].Add(msg.Data);
								messageCounts.AddOrUpdate(consumerId, 1, (k, v) => v + 1);

								bool acknowledged = false;
								for (int attempt = 1; attempt <= 3; attempt++)
								{
									try
									{
										await msg.Consumer.Acknowledge(msg.MessageId); // Use DotPulsar Acknowledge
										acknowledged = true;
										Console.WriteLine($"[Consumer-{consumerId}] Acknowledged message.");
										break;
									}
									catch (Exception ex)
									{
										Console.WriteLine($"[Consumer-{consumerId}] Ack attempt {attempt}/3 failed: {ex.Message}");
										if (attempt < 3)
											await Task.Delay(100);
									}
								}

								if (!acknowledged)
								{
									Console.WriteLine($"[Consumer-{consumerId}] Failed to acknowledge message after 3 attempts.");
								}

								await Task.Delay(50); // Balance distribution

								if (messageCounts.Values.Sum() == 30)
								{
									lock (cancellationLock)
									{
										if (!cancellationTriggered)
										{
											cancellationTriggered = true;
											Console.WriteLine($"[Consumer-{consumerId}] Exactly 30 messages processed, preparing to cancel.");
											pendingAcks[consumerId].TrySetResult(true);
										}
									}
								}
							}
							catch (Exception ex)
							{
								Console.WriteLine($"[Consumer-{consumerId}] Error processing message: {ex.Message}");
							}
							finally
							{
								if (messageCounts.Values.Sum() >= 30)
								{
									pendingAcks[consumerId].TrySetResult(true);
								}
							}
						}, cts.Token);
					}
					catch (OperationCanceledException)
					{
						Console.WriteLine($"[Consumer-{consumerId}] Subscription canceled.");
						pendingAcks[consumerId].TrySetResult(true);
					}
					catch (Exception ex)
					{
						Console.WriteLine($"[Consumer-{consumerId}] Subscription failed: {ex.Message}");
						pendingAcks[consumerId].TrySetResult(true);
					}
				}

				// Start consumers
				var consumerTasks = Enumerable.Range(1, 3)
					.Select(id => Task.Run(() => RunConsumer(id), cts.Token))
					.ToArray();

				// Wait for all consumers to subscribe
				await Task.WhenAll(readySignals.Values.Select(tcs => tcs.Task));
				Console.WriteLine("[SETUP] All consumers subscribed.");

				// Connect producer
				var producerData = new ProducerData
				{
					ServiceUrl = _serviceUrl,
					TopicName = $"persistent://public/default/{topicName}",
					EnableBatching = false
				};
				await _producerManager.ConnectProducer(producerData);
				Console.WriteLine("[SETUP] Producer connected.");

				// Send 30 messages
				for (int i = 1; i <= 30; i++)
				{
					var message = $"{{ \"taskId\": {i}, \"type\": \"process\" }}";
					await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
					metrics.MessagesSent++;
					Console.WriteLine($"[PRODUCER] Sent message {i}: {message}");
					await Task.Delay(50);
				}

				// Wait for consumers with timeout
				try
				{
					await Task.WhenAny(
						Task.WhenAll(consumerTasks),
						Task.WhenAll(pendingAcks.Values.Select(tcs => tcs.Task)),
						Task.Delay(TimeSpan.FromSeconds(30), cts.Token)
					);
					if (!cts.IsCancellationRequested)
					{
						Console.WriteLine("[WARNING] Timeout waiting for consumers, canceling.");
						cts.Cancel();
					}
					await Task.WhenAll(pendingAcks.Values.Select(tcs => tcs.Task));
					cts.Cancel();
					await Task.WhenAll(consumerTasks);
				}
				catch (OperationCanceledException)
				{
					Console.WriteLine("[INFO] Consumers completed processing.");
				}

				// Display results
				Console.WriteLine("\n=== Pulsar Demo 2 Results ===");
				Console.WriteLine($"Topic: {topicName}");
				Console.WriteLine($"Subscription: shared-sub");
				Console.WriteLine("Consumer   | Messages Received | Sample Messages");
				Console.WriteLine("-----------|-------------------|----------------");
				foreach (var id in messageCounts.Keys.OrderBy(k => k))
				{
					var count = messageCounts.GetValueOrDefault(id, 0);
					var sample = messagesReceived[id].Take(2).Any() ? string.Join(", ", messagesReceived[id].Take(2)) : "None";
					Console.WriteLine($"Consumer-{id,-1} | {count,-17} | {sample}");
				}

				stopwatch.Stop();
				metrics.SetupTime = stopwatch.Elapsed;
				metrics.MessagesReceived = messageCounts.Values.Sum();
				metrics.Notes += "Shared subscription message distribution; " +
								 string.Join("; ", messageCounts.OrderBy(kv => kv.Key).Select(kv => $"Consumer-{kv.Key}: {kv.Value} messages"));
				metrics.Display("Pulsar Demo 2");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"[ERROR] Pulsar Demo 2 failed: {ex.Message}");
				metrics.Notes += $"Failed: {ex.Message}";
				metrics.Display("Pulsar Demo 2");
			}
			finally
			{
				foreach (var manager in consumerManagers)
				{
					await manager.DisposeAsync();
				}
				_producerManager.Dispose();
				Console.WriteLine("[INFO] Resources disposed.");
			}
		}

		//public async Task RunDemo3()
		//{
		//	var metrics = new DemoMetrics { DemoName = "Topic Hotspot Auto-Splitting" };
		//	var stopwatch = Stopwatch.StartNew();
		//	Console.WriteLine("\n=== Demo 3: Topic Hotspot Auto-Splitting (Pulsar) ===");
		//	Console.WriteLine("Goal: Show Pulsar's ability to redistribute load from a hotspot by creating a new topic to balance consumers.\n");

		//	List<ConsumerManager> consumerManagers = new();
		//	ConcurrentBag<string> errors = new();
		//	string hotTopicName = null;
		//	string splitTopicName = null;
		//	var messageCounts = new ConcurrentDictionary<int, int>();
		//	var messagesReceived = new ConcurrentDictionary<int, List<string>>();

		//	try
		//	{
		//		// Create hot topic
		//		var baseTopicName = $"OrderDemo3_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
		//		hotTopicName = $"{baseTopicName}_hot";
		//		var topicData = new CreateTopicData(_serviceUrl, hotTopicName)
		//		{
		//			Tenant = "public",
		//			Namespace = "default",
		//			NumPartitions = 1,
		//			RetentionMins = "10080"
		//		};
		//		var topicResponse = await _topicManager.CreateTopic(topicData);
		//		if (!topicResponse.IsSuccess)
		//		{
		//			throw new Exception($"Hot topic creation failed: {topicResponse.Message}");
		//		}
		//		Console.WriteLine($"[SETUP] {topicResponse.Message}");
		//		metrics.Notes += $"Hot topic creation: {topicResponse.Message}; ";

		//		// Prepare
		//		var hotCts = new CancellationTokenSource();
		//		var splitCts = new CancellationTokenSource();
		//		var readySignals = new Dictionary<int, TaskCompletionSource<bool>>();
		//		var pendingAcks = new ConcurrentDictionary<int, TaskCompletionSource<bool>>();
		//		var pendingMessages = new TaskCompletionSource<bool>();
		//		var cancellationLock = new object();
		//		bool cancellationTriggered = false;

		//		for (int i = 1; i <= 3; i++)
		//		{
		//			messageCounts[i] = 0;
		//			messagesReceived[i] = new List<string>();
		//			readySignals[i] = new TaskCompletionSource<bool>();
		//			pendingAcks[i] = new TaskCompletionSource<bool>();
		//		}

		//		// Consumer logic
		//		async Task RunConsumer(int consumerId, string splitTopic)
		//		{
		//			var consumerManager = new ConsumerManager(
		//				errorHandler: (c, ex) =>
		//				{
		//					string error = $"[ERROR] Consumer-{consumerId}: {ex.Message}";
		//					Console.WriteLine(error);
		//					errors.Add(error);
		//				},
		//				subscriptionType: SubscriptionType.Shared,
		//				messagePrefetchCount: 10
		//			);
		//			lock (consumerManagers)
		//			{
		//				consumerManagers.Add(consumerManager);
		//			}

		//			try
		//			{
		//				// Hot topic subscription
		//				var hotConsumerData = new ConsumerData
		//				{
		//					ServiceUrl = _serviceUrl,
		//					TopicName = $"persistent://public/default/{hotTopicName}",
		//					SubscriptionName = "shared-sub",
		//					SubscriptionType = SubscriptionType.Shared
		//				};
		//				consumerManager.ConnectConsumer(hotConsumerData, (c, ex) =>
		//					Console.WriteLine($"[ERROR] Consumer-{consumerId}: {ex.Message}"));

		//				Console.WriteLine($"[Consumer-{consumerId}] Subscribing to {hotTopicName}...");
		//				await consumerManager.SubscribeAsync(hotConsumerData.TopicName, async msg =>
		//				{
		//					try
		//					{
		//						if (!string.IsNullOrEmpty(msg.ExceptionMessage))
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
		//							return;
		//						}
		//						if (msg.MessageId == null || string.IsNullOrEmpty(msg.Data))
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Warning: Invalid message, skipping.");
		//							return;
		//						}

		//						if (messageCounts.Values.Sum() >= 50)
		//						{
		//							return;
		//						}

		//						Console.ForegroundColor = consumerId switch
		//						{
		//							1 => ConsoleColor.Cyan,
		//							2 => ConsoleColor.Magenta,
		//							3 => ConsoleColor.Yellow,
		//							_ => ConsoleColor.White
		//						};
		//						Console.WriteLine($"[Consumer-{consumerId}] Received: {msg.Data} (MessageId: {msg.MessageId})");
		//						Console.ResetColor();

		//						messagesReceived[consumerId].Add(msg.Data);
		//						messageCounts.AddOrUpdate(consumerId, 1, (k, v) => v + 1);

		//						bool acknowledged = false;
		//						for (int attempt = 1; attempt <= 3; attempt++)
		//						{
		//							try
		//							{
		//								await msg.Consumer.Acknowledge(msg.MessageId);
		//								acknowledged = true;
		//								Console.WriteLine($"[Consumer-{consumerId}] Acknowledged message.");
		//								break;
		//							}
		//							catch (Exception ex)
		//							{
		//								Console.WriteLine($"[Consumer-{consumerId}] Ack attempt {attempt}/3 failed: {ex.Message}");
		//								if (attempt < 3)
		//									await Task.Delay(100);
		//							}
		//						}

		//						if (!acknowledged)
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Failed to acknowledge message after 3 attempts.");
		//						}

		//						// Signal hotspot progress
		//						if (messageCounts.Values.Sum() >= 20 && !pendingMessages.Task.IsCompleted)
		//						{
		//							pendingMessages.TrySetResult(true);
		//						}

		//						if (messageCounts.Values.Sum() == 50)
		//						{
		//							lock (cancellationLock)
		//							{
		//								if (!cancellationTriggered)
		//								{
		//									cancellationTriggered = true;
		//									Console.WriteLine($"[Consumer-{consumerId}] Exactly 50 messages processed, preparing to cancel.");
		//									pendingAcks[consumerId].TrySetResult(true);
		//								}
		//							}
		//						}
		//					}
		//					catch (Exception ex)
		//					{
		//						string error = $"[Consumer-{consumerId}] Error processing message: {ex.Message}";
		//						Console.WriteLine(error);
		//						errors.Add(error);
		//					}
		//					finally
		//					{
		//						if (messageCounts.Values.Sum() >= 50)
		//						{
		//							pendingAcks[consumerId].TrySetResult(true);
		//						}
		//					}
		//				}, hotCts.Token);

		//				Console.WriteLine($"[Consumer-{consumerId}] Setting ready signal for hot topic...");
		//				readySignals[consumerId].SetResult(true);
		//				Console.WriteLine($"[Consumer-{consumerId}] Ready signal set for hot topic.");

		//				// Wait for split phase
		//				await pendingMessages.Task;

		//				if (!hotCts.IsCancellationRequested)
		//				{
		//					hotCts.Cancel();
		//					await Task.Delay(100); // Allow hot phase to settle
		//				}

		//				// Split topic subscription
		//				var splitConsumerData = new ConsumerData
		//				{
		//					ServiceUrl = _serviceUrl,
		//					TopicName = $"persistent://public/default/{splitTopic}",
		//					SubscriptionName = "shared-sub",
		//					SubscriptionType = SubscriptionType.Shared
		//				};
		//				Console.WriteLine($"[Consumer-{consumerId}] Subscribing to {splitTopic}...");
		//				await consumerManager.SubscribeAsync(splitConsumerData.TopicName, async msg =>
		//				{
		//					try
		//					{
		//						if (!string.IsNullOrEmpty(msg.ExceptionMessage))
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
		//							return;
		//						}
		//						if (msg.MessageId == null || string.IsNullOrEmpty(msg.Data))
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Warning: Invalid message, skipping.");
		//							return;
		//						}

		//						if (messageCounts.Values.Sum() >= 50)
		//						{
		//							return;
		//						}

		//						Console.ForegroundColor = consumerId switch
		//						{
		//							1 => ConsoleColor.Cyan,
		//							2 => ConsoleColor.Magenta,
		//							3 => ConsoleColor.Yellow,
		//							_ => ConsoleColor.White
		//						};
		//						Console.WriteLine($"[Consumer-{consumerId}] Received: {msg.Data} (MessageId: {msg.MessageId})");
		//						Console.ResetColor();

		//						messagesReceived[consumerId].Add(msg.Data);
		//						messageCounts.AddOrUpdate(consumerId, 1, (k, v) => v + 1);

		//						bool acknowledged = false;
		//						for (int attempt = 1; attempt <= 3; attempt++)
		//						{
		//							try
		//							{
		//								await msg.Consumer.Acknowledge(msg.MessageId);
		//								acknowledged = true;
		//								Console.WriteLine($"[Consumer-{consumerId}] Acknowledged message.");
		//								break;
		//							}
		//							catch (Exception ex)
		//							{
		//								Console.WriteLine($"[Consumer-{consumerId}] Ack attempt {attempt}/3 failed: {ex.Message}");
		//								if (attempt < 3)
		//									await Task.Delay(100);
		//							}
		//						}

		//						if (!acknowledged)
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Failed to acknowledge message after 3 attempts.");
		//						}

		//						if (messageCounts.Values.Sum() == 50)
		//						{
		//							lock (cancellationLock)
		//							{
		//								if (!cancellationTriggered)
		//								{
		//									cancellationTriggered = true;
		//									Console.WriteLine($"[Consumer-{consumerId}] Exactly 50 messages processed, preparing to cancel.");
		//									pendingAcks[consumerId].TrySetResult(true);
		//								}
		//							}
		//						}
		//					}
		//					catch (Exception ex)
		//					{
		//						string error = $"[Consumer-{consumerId}] Error processing message: {ex.Message}";
		//						Console.WriteLine(error);
		//						errors.Add(error);
		//					}
		//					finally
		//					{
		//						if (messageCounts.Values.Sum() >= 50)
		//						{
		//							pendingAcks[consumerId].TrySetResult(true);
		//						}
		//					}
		//				}, splitCts.Token);
		//			}
		//			catch (OperationCanceledException)
		//			{
		//				Console.WriteLine($"[Consumer-{consumerId}] Subscription canceled.");
		//				pendingAcks[consumerId].TrySetResult(true);
		//			}
		//			catch (Exception ex)
		//			{
		//				string error = $"[Consumer-{consumerId}] Subscription failed: {ex.Message}";
		//				Console.WriteLine(error);
		//				errors.Add(error);
		//				pendingAcks[consumerId].TrySetResult(true);
		//			}
		//		}

		//		// Start consumers
		//		splitTopicName = $"{baseTopicName}_split";
		//		var consumerTasks = new Task[3];
		//		for (int id = 1; id <= 3; id++)
		//		{
		//			int consumerId = id; // Capture for async
		//			consumerTasks[id - 1] = RunConsumer(consumerId, splitTopicName);
		//		}

		//		// Wait for hot topic subscriptions with timeout
		//		try
		//		{
		//			var readyTask = Task.WhenAll(readySignals.Values.Select(tcs => tcs.Task));
		//			await Task.WhenAny(readyTask, Task.Delay(TimeSpan.FromSeconds(10), hotCts.Token));
		//			if (!readyTask.IsCompleted)
		//			{
		//				Console.WriteLine("[ERROR] Timeout waiting for consumer subscriptions.");
		//				errors.Add("Timeout waiting for consumer subscriptions");
		//				hotCts.Cancel();
		//				throw new Exception("Consumer subscriptions timed out");
		//			}
		//			Console.WriteLine("[SETUP] All consumers subscribed to hot topic.");
		//		}
		//		catch (Exception ex)
		//		{
		//			errors.Add($"Subscription setup failed: {ex.Message}");
		//			throw;
		//		}

		//		// Connect producer for hot topic
		//		var producerData = new ProducerData
		//		{
		//			ServiceUrl = _serviceUrl,
		//			TopicName = $"persistent://public/default/{hotTopicName}",
		//			EnableBatching = false
		//		};
		//		await _producerManager.ConnectProducer(producerData);
		//		Console.WriteLine("[SETUP] Producer connected to hot topic.");

		//		// Send hotspot messages (1–25)
		//		for (int i = 1; i <= 25; i++)
		//		{
		//			var message = $"{{ \"taskId\": {i}, \"type\": \"process\", \"hotspot\": true }}";
		//			await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
		//			metrics.MessagesSent++;
		//			Console.WriteLine($"[PRODUCER] Sent message {i}: {message}");
		//			await Task.Delay(50);
		//		}

		//		// Wait for hotspot messages
		//		await Task.WhenAny(
		//			pendingMessages.Task,
		//			Task.Delay(TimeSpan.FromSeconds(10), hotCts.Token)
		//		);
		//		if (!pendingMessages.Task.IsCompleted)
		//		{
		//			Console.WriteLine("[WARNING] Timeout waiting for hotspot messages, proceeding to split.");
		//			errors.Add("Timeout waiting for hotspot messages");
		//		}

		//		// Create split topic
		//		Console.WriteLine("[SETUP] Detected hotspot, creating new topic to balance load...");
		//		topicData = new CreateTopicData(_serviceUrl, splitTopicName)
		//		{
		//			Tenant = "public",
		//			Namespace = "default",
		//			NumPartitions = 3,
		//			RetentionMins = "10080"
		//		};
		//		topicResponse = await _topicManager.CreateTopic(topicData);
		//		if (!topicResponse.IsSuccess)
		//		{
		//			throw new Exception($"Split topic creation failed: {topicResponse.Message}");
		//		}
		//		Console.WriteLine($"[SETUP] {topicResponse.Message}");
		//		metrics.Notes += $"Split topic creation: {topicResponse.Message}; ";

		//		// Switch producer to split topic
		//		_producerManager.Dispose();
		//		producerData = new ProducerData
		//		{
		//			ServiceUrl = _serviceUrl,
		//			TopicName = $"persistent://public/default/{splitTopicName}",
		//			EnableBatching = false
		//		};
		//		await _producerManager.ConnectProducer(producerData);
		//		Console.WriteLine("[SETUP] Producer connected to split topic.");

		//		// Send split messages (26–50)
		//		for (int i = 26; i <= 50; i++)
		//		{
		//			var message = $"{{ \"taskId\": {i}, \"type\": \"process\", \"hotspot\": false }}";
		//			await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
		//			metrics.MessagesSent++;
		//			Console.WriteLine($"[PRODUCER] Sent message {i}: {message}");
		//			await Task.Delay(50);
		//		}

		//		// Wait for completion
		//		try
		//		{
		//			await Task.WhenAny(
		//				Task.WhenAll(consumerTasks),
		//				Task.WhenAll(pendingAcks.Values.Select(tcs => tcs.Task)),
		//				Task.Delay(TimeSpan.FromSeconds(30), splitCts.Token)
		//			);
		//			if (!splitCts.IsCancellationRequested)
		//			{
		//				Console.WriteLine("[WARNING] Timeout waiting for consumers, canceling.");
		//				errors.Add("Timeout waiting for consumers");
		//				splitCts.Cancel();
		//			}
		//			await Task.WhenAll(pendingAcks.Values.Select(tcs => tcs.Task));
		//		}
		//		catch (OperationCanceledException)
		//		{
		//			Console.WriteLine("[INFO] Consumers completed processing.");
		//		}
		//		catch (Exception ex)
		//		{
		//			errors.Add($"Completion error: {ex.Message}");
		//		}
		//	}
		//	catch (Exception ex)
		//	{
		//		Console.WriteLine($"[ERROR] Pulsar Demo 3 failed: {ex.Message}");
		//		errors.Add($"Demo failed: {ex.Message}");
		//	}
		//	finally
		//	{
		//		// Display results
		//		Console.WriteLine("\n=== Pulsar Demo 3 Results ===");
		//		Console.WriteLine($"Hot Topic: {hotTopicName ?? "Not created"}");
		//		Console.WriteLine($"Split Topic: {splitTopicName ?? "Not created"}");
		//		Console.WriteLine($"Subscription: shared-sub");
		//		Console.WriteLine("Consumer   | Messages Received | Sample Messages");
		//		Console.WriteLine("-----------|-------------------|----------------");
		//		foreach (var id in messageCounts.Keys.OrderBy(k => k))
		//		{
		//			var count = messageCounts.GetValueOrDefault(id, 0);
		//			var sample = messagesReceived[id].Take(2).Any() ? string.Join(", ", messagesReceived[id].Take(2)) : "None";
		//			Console.WriteLine($"Consumer-{id,-1} | {count,-17} | {sample}");
		//		}

		//		stopwatch.Stop();
		//		metrics.SetupTime = stopwatch.Elapsed;
		//		metrics.MessagesReceived = messageCounts.Values.Sum();
		//		metrics.Notes += $"Total messages received: {metrics.MessagesReceived}; " +
		//						 string.Join("; ", messageCounts.OrderBy(kv => kv.Key).Select(kv => $"Consumer-{kv.Key}: {kv.Value} messages"));
		//		if (errors.Any())
		//		{
		//			metrics.Notes += $"; Errors: {string.Join("; ", errors)}";
		//		}
		//		metrics.Display("Pulsar Demo 3");

		//		foreach (var manager in consumerManagers)
		//		{
		//			await manager.DisposeAsync();
		//		}
		//		_producerManager.Dispose();
		//		Console.WriteLine("[INFO] Resources disposed.");
		//	}
		//}
		public async Task RunDemo4()
		{
			Console.WriteLine("\n=== Demo 4: Per-Message Acknowledgment & Redelivery ===");
			Console.WriteLine("Goal: Compare Pulsar's per-message acknowledgment with Kafka's offset-based commits.\n");

			await RunPulsarDemo4();
		}
		private async Task RunPulsarDemo4()
		{
			var metrics = new DemoMetrics { DemoName = "Per-Message Acknowledgment (Pulsar)" };
			var stopwatch = Stopwatch.StartNew();
			Console.WriteLine("\n=== Pulsar Demo 4 ===");
			Console.WriteLine("Scenario: Send 10 messages, fail on #5 twice, show redelivery of only #5.\n");

			ConsumerManager consumerManager = null;
			ConcurrentBag<string> errors = new();
			var messageCounts = new ConcurrentDictionary<string, int>();
			var messagesReceived = new ConcurrentDictionary<string, List<string>>();
			var redeliveryCounts = new ConcurrentDictionary<string, int>();
			var task5ReceiveCount = 0;
			string topicName = null;

			try
			{
				topicName = $"OrderDemo4_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
				var topicData = new CreateTopicData(_serviceUrl, topicName)
				{
					Tenant = "public",
					Namespace = "default",
					NumPartitions = 1,
					RetentionMins = "10080"
				};
				var topicResponse = await _topicManager.CreateTopic(topicData);
				if (!topicResponse.IsSuccess)
					throw new Exception($"Topic creation failed: {topicResponse.Message}");

				Console.WriteLine($"[SETUP] {topicResponse.Message}");
				metrics.Notes += $"Topic creation: {topicResponse.Message}; ";

				var cts = new CancellationTokenSource();
				messageCounts["Consumer-1"] = 0;
				messagesReceived["Consumer-1"] = new List<string>();
				redeliveryCounts["Consumer-1"] = 0;

				async Task RunConsumer()
				{
					consumerManager = new ConsumerManager(
						errorHandler: (c, ex) =>
						{
							string error = $"[ERROR] Consumer-1: {ex.Message}";
							Console.WriteLine(error);
							errors.Add(error);
						},
						subscriptionType: SubscriptionType.Exclusive,
						messagePrefetchCount: 10
					);

					var consumerData = new ConsumerData
					{
						ServiceUrl = _serviceUrl,
						TopicName = $"persistent://public/default/{topicName}",
						SubscriptionName = "exclusive-sub",
						SubscriptionType = SubscriptionType.Exclusive
					};

					consumerManager.ConnectConsumer(consumerData, (c, ex) =>
						Console.WriteLine($"[ERROR] Consumer-1: {ex.Message}"));

					Console.WriteLine("[Consumer-1] Subscribing...");
					await consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
					{
						try
						{
							if (!string.IsNullOrEmpty(msg.ExceptionMessage))
							{
								Console.WriteLine($"[Consumer-1] Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
								return;
							}

							if (msg.MessageId == null || string.IsNullOrEmpty(msg.Data))
							{
								Console.WriteLine($"[Consumer-1] Warning: Invalid message, skipping.");
								return;
							}

							var taskId = msg.Data.Contains("taskId") ? int.Parse(msg.Data.Split("\"taskId\": ")[1].Split('}')[0]) : 0;
							Console.ForegroundColor = ConsoleColor.Cyan;
							Console.WriteLine($"[Consumer-1] Received: {msg.Data} (MessageId: {msg.MessageId})");
							Console.ResetColor();

							messagesReceived["Consumer-1"].Add(msg.Data);
							messageCounts.AddOrUpdate("Consumer-1", 1, (k, v) => v + 1);

							if (taskId == 5)
							{
								task5ReceiveCount++;
								redeliveryCounts.AddOrUpdate("Consumer-1", 1, (k, v) => v + 1);
								if (task5ReceiveCount <= 2)
								{
									Console.WriteLine($"[Consumer-1] Simulating failure on message #5 (attempt {task5ReceiveCount}/2).");

									// ✅ Trigger redelivery for only this message
									await msg.Consumer.RedeliverUnacknowledgedMessages(new[] { msg.MessageId });

									throw new Exception("Simulated processing failure for message #5");
								}
							}


							await msg.Consumer.Acknowledge(msg.MessageId);
							Console.WriteLine($"[Consumer-1] Acknowledged message.");

							// Stop when all messages processed and #5 was seen 3 times
							if (messageCounts["Consumer-1"] >= 12 && task5ReceiveCount >= 3)
							{
								Console.WriteLine("[Consumer-1] All expected messages processed, cancelling.");
								cts.Cancel();
							}
						}
						catch (Exception ex)
						{
							string error = $"[Consumer-1] Error processing message: {ex.Message}";
							Console.WriteLine(error);
							errors.Add(error);
						}
					}, cts.Token);
				}

				var consumerTask = Task.Run(() => RunConsumer(), cts.Token);
				await Task.Delay(1000, cts.Token); // Allow subscription
				Console.WriteLine("[SETUP] Consumer subscribed.");

				var producerData = new ProducerData
				{
					ServiceUrl = _serviceUrl,
					TopicName = $"persistent://public/default/{topicName}",
					EnableBatching = false
				};
				await _producerManager.ConnectProducer(producerData);
				Console.WriteLine("[SETUP] Producer connected.");

				for (int i = 1; i <= 10; i++)
				{
					var message = $"{{ \"taskId\": {i} }}";
					await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
					metrics.MessagesSent++;
					Console.WriteLine($"[PRODUCER] Sent message {i}: {message}");
					await Task.Delay(100, cts.Token);
				}

				try
				{
					await Task.WhenAny(consumerTask, Task.Delay(TimeSpan.FromSeconds(30), cts.Token));
					if (!cts.IsCancellationRequested)
					{
						Console.WriteLine("[WARNING] Timeout waiting for consumer, canceling.");
						errors.Add("Timeout waiting for consumer");
						cts.Cancel();
					}
					await consumerTask;
				}
				catch (OperationCanceledException)
				{
					Console.WriteLine("[INFO] Consumer completed processing.");
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"[ERROR] Pulsar Demo 4 failed: {ex.Message}");
				errors.Add($"Demo failed: {ex.Message}");
			}
			finally
			{
				Console.WriteLine("\n=== Pulsar Demo 4 Results ===");
				Console.WriteLine($"Topic: {topicName ?? "Not created"}");
				Console.WriteLine($"Subscription: exclusive-sub");
				Console.WriteLine("Consumer   | Messages Received | Redeliveries | Sample Messages");
				Console.WriteLine("-----------|-------------------|--------------|----------------");
				var count = messageCounts.GetValueOrDefault("Consumer-1", 0);
				var redeliveries = redeliveryCounts.GetValueOrDefault("Consumer-1", 0);
				var sample = messagesReceived["Consumer-1"].Take(2).Any() ? string.Join(", ", messagesReceived["Consumer-1"].Take(2)) : "None";
				Console.WriteLine($"Consumer-1 | {count,-17} | {redeliveries,-12} | {sample}");

				stopwatch.Stop();
				metrics.SetupTime = stopwatch.Elapsed;
				metrics.MessagesReceived = messageCounts.Values.Sum();
				metrics.Notes += $"Total messages received: {metrics.MessagesReceived}; Redeliveries: {redeliveries}; ";
				if (errors.Any())
					metrics.Notes += $"Errors: {string.Join("; ", errors)}";

				metrics.Display("Pulsar Demo 4");

				if (consumerManager != null)
					await consumerManager.DisposeAsync();

				_producerManager.Dispose();
				Console.WriteLine("[INFO] Resources disposed.");
			}
		}






		//public async Task RunDemo1()
		//{
		//	var metrics = new DemoMetrics();
		//	var stopwatch = System.Diagnostics.Stopwatch.StartNew();
		//	Console.WriteLine("=== Demo 1: Multiple Subscriptions on the Same Topic ===");

		//	try
		//	{
		//		// Create topic
		//		var topicData = new CreateTopicData(_serviceUrl, "OrderFifAprTwo22")
		//		{
		//			Tenant = "public",
		//			Namespace = "default",
		//			NumPartitions = 1
		//		};
		//		var topicResponse = await _topicManager.CreateTopic(topicData);
		//		Console.WriteLine(topicResponse.Message);
		//		metrics.Notes += $"Topic creation: {topicResponse.Message}; ";

		//		// Create consumers
		//		var subscriptions = new[] { "one-sub", "two-sub", "three-sub" };
		//		var cts = new CancellationTokenSource();
		//		var messageCounts = new ConcurrentDictionary<string, int>();

		//		foreach (var sub in subscriptions)
		//			messageCounts[sub] = 0;

		//		async Task RunConsumer(string subName)
		//		{
		//			var consumerData = new ConsumerData
		//			{
		//				ServiceUrl = _serviceUrl,
		//				TopicName = "persistent://public/default/OrderFifAprTwo22",
		//				SubscriptionName = subName,
		//				SubscriptionType = SubscriptionType.Exclusive,
		//			};

		//			int localCount = 0;

		//			try
		//			{
		//				// 🔥 Create isolated ConsumerManager for this consumer
		//				var isolatedConsumerManager = new ConsumerManager(); // or however you normally instantiate it
		//				isolatedConsumerManager.ConnectConsumer(consumerData, (c, ex) =>
		//					Console.WriteLine($"[ERROR] Consumer {subName}: {ex.Message}"));

		//				Console.WriteLine($"[{subName}] Subscribing...");
		//				await isolatedConsumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
		//				{
		//					try
		//					{
		//						if (!string.IsNullOrEmpty(msg.ExceptionMessage))
		//						{
		//							Console.WriteLine($"[{subName}] Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
		//							return;
		//						}
		//						if (msg.MessageId == null)
		//						{
		//							Console.WriteLine($"[{subName}] Warning: Null MessageId, skipping.");
		//							return;
		//						}
		//						if (string.IsNullOrEmpty(msg.Data))
		//						{
		//							Console.WriteLine($"[{subName}] Warning: Empty message data, skipping.");
		//							return;
		//						}

		//						Console.WriteLine($"[{subName}] Received: {msg.Data}");
		//						localCount++;

		//						//await isolatedConsumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId });
		//						await msg.Consumer.Acknowledge(msg.MessageId);

		//						Console.WriteLine($"[{subName}] Acknowledged message.");

		//						messageCounts.AddOrUpdate(subName, localCount, (_, _) => localCount);

		//						if (localCount >= 10)
		//						{
		//							Console.WriteLine($"[{subName}] Received 10 messages, signaling completion.");

		//							var snapshot = messageCounts.ToArray();
		//							if (snapshot.All(kv => kv.Value >= 10))
		//							{
		//								Console.WriteLine($"[{subName}] All subscriptions completed, canceling.");
		//								cts.Cancel();
		//							}
		//						}
		//					}
		//					catch (Exception ex)
		//					{
		//						Console.WriteLine($"[{subName}] Error processing message: {ex.Message}");
		//					}
		//				}, cts.Token);

		//				Console.WriteLine($"[{subName}] Subscription completed.");
		//			}
		//			catch (OperationCanceledException)
		//			{
		//				Console.WriteLine($"[{subName}] Subscription canceled.");
		//				messageCounts.AddOrUpdate(subName, localCount, (_, _) => localCount);
		//			}
		//			catch (Exception ex)
		//			{
		//				Console.WriteLine($"[{subName}] Subscription failed: {ex.Message}");
		//			}
		//		}

		//		// Start consumers
		//		var consumerTasks = subscriptions.Select(sub => Task.Run(() => RunConsumer(sub), cts.Token)).ToArray();
		//		await Task.Delay(1000, cts.Token);

		//		// Connect producer
		//		var producerData = new ProducerData
		//		{
		//			ServiceUrl = _serviceUrl,
		//			TopicName = "persistent://public/default/OrderFifAprTwo22",
		//			EnableBatching = false
		//		};
		//		await _producerManager.ConnectProducer(producerData);
		//		Console.WriteLine("Producer connected.");

		//		// Send 10 messages
		//		for (int i = 1; i <= 10; i++)
		//		{
		//			var message = $"{{ \"orderId\": {i}, \"amount\": {i * 100} }}";
		//			await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
		//			metrics.MessagesSent++;
		//			Console.WriteLine($"Sent message {i}: {message}");
		//			await Task.Delay(100);
		//		}

		//		// Wait for consumers
		//		try
		//		{
		//			await Task.WhenAll(consumerTasks);
		//		}
		//		catch (OperationCanceledException)
		//		{
		//			Console.WriteLine("[INFO] Consumer tasks canceled after processing.");
		//		}

		//		stopwatch.Stop();
		//		metrics.SetupTime = stopwatch.Elapsed;
		//		metrics.MessagesReceived = messageCounts.Values.Sum();
		//		metrics.Notes += $"Total messages received: {metrics.MessagesReceived}; " +
		//						string.Join(", ", messageCounts.Select(kv => $"{kv.Key}: {kv.Value} messages"));
		//		metrics.Display("Demo 1");
		//	}
		//	catch (Exception ex)
		//	{
		//		Console.WriteLine($"Demo 1 failed: {ex.Message}");
		//		metrics.Notes += $"Failed: {ex.Message}";
		//		metrics.Display("Demo 1");
		//	}
		//	finally
		//	{
		//		await _consumerManager.DisposeAsync();
		//	}
		//}

		//public async Task RunDemo1()
		//{
		//	var metrics = new DemoMetrics();
		//	var stopwatch = System.Diagnostics.Stopwatch.StartNew();
		//	Console.WriteLine("=== Demo 1: Single Subscription on a Topic ===");

		//	try
		//	{
		//		// Create topic
		//		var topicData = new CreateTopicData(_serviceUrl, "Order2")
		//		{
		//			Tenant = "public",
		//			Namespace = "default",
		//			NumPartitions = 1
		//		};
		//		var topicResponse = await _topicManager.CreateTopic(topicData);
		//		Console.WriteLine(topicResponse.Message);
		//		metrics.Notes += $"Topic creation: {topicResponse.Message}; ";

		//		// Connect producer
		//		var producerData = new ProducerData
		//		{
		//			ServiceUrl = _serviceUrl,
		//			TopicName = "persistent://public/default/Order2",
		//			EnableBatching = false
		//		};
		//		await _producerManager.ConnectProducer(producerData);
		//		Console.WriteLine("Producer connected.");

		//		// Send 10 messages
		//		for (int i = 1; i <= 10; i++)
		//		{
		//			var message = $"{{ \"orderId\": {i}, \"amount\": {i * 100} }}";
		//			await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
		//			metrics.MessagesSent++;
		//			Console.WriteLine($"Sent message {i}: {message}");
		//			await Task.Delay(100);
		//		}

		//		// Create consumer
		//		var subscriptions = new[] { "billing-sub" };
		//		var cts = new CancellationTokenSource();
		//		var messageCount = 0;

		//		async Task RunConsumer(string subName)
		//		{
		//			var consumerData = new ConsumerData
		//			{
		//				ServiceUrl = _serviceUrl,
		//				TopicName = "persistent://public/default/Order2",
		//				SubscriptionName = subName,
		//				SubscriptionType = SubscriptionType.Exclusive,
		//			};

		//			try
		//			{
		//				Console.WriteLine($"[{subName}] Connecting consumer...");
		//				_consumerManager.ConnectConsumer(consumerData, (c, ex) =>
		//					Console.WriteLine($"[ERROR] Consumer {subName}: {ex.Message}"));

		//				Console.WriteLine($"[{subName}] Subscribing...");
		//				await _consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
		//				{
		//					try
		//					{
		//						if (!string.IsNullOrEmpty(msg.ExceptionMessage))
		//						{
		//							Console.WriteLine($"[{subName}] Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
		//							return;
		//						}
		//						if (msg.MessageId == null)
		//						{
		//							Console.WriteLine($"[{subName}] Warning: Null MessageId, skipping.");
		//							return;
		//						}
		//						if (string.IsNullOrEmpty(msg.Data))
		//						{
		//							Console.WriteLine($"[{subName}] Warning: Empty message data, skipping.");
		//							return;
		//						}
		//						Console.WriteLine($"[{subName}] Received: {msg.Data}");
		//						Interlocked.Increment(ref messageCount);
		//						await _consumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId });
		//						Console.WriteLine($"[{subName}] Acknowledged message.");
		//						if (messageCount >= 10)
		//						{
		//							Console.WriteLine($"[{subName}] Received 10 messages, canceling.");
		//							cts.Cancel();
		//						}
		//					}
		//					catch (Exception ex)
		//					{
		//						Console.WriteLine($"[{subName}] Error processing message: {ex.Message}");
		//					}
		//				}, cts.Token);
		//				Console.WriteLine($"[{subName}] Subscription completed.");
		//			}
		//			catch (OperationCanceledException)
		//			{
		//				Console.WriteLine($"[{subName}] Subscription canceled.");
		//			}
		//			catch (Exception ex)
		//			{
		//				Console.WriteLine($"[{subName}] Subscription failed: {ex.Message}");
		//			}
		//		}

		//		// Start consumer before producing (optional, since Earliest ensures delivery)
		//		var consumerTasks = subscriptions.Select(sub => Task.Run(() => RunConsumer(sub), cts.Token)).ToArray();
		//		await Task.Delay(1000, cts.Token); // Brief wait for subscription

		//		await Task.WhenAll(consumerTasks);
		//		stopwatch.Stop();
		//		metrics.SetupTime = stopwatch.Elapsed;
		//		metrics.MessagesReceived = messageCount;
		//		metrics.Notes += $"Total messages received: {messageCount}; billing-sub: {messageCount} messages";
		//		metrics.Display("Demo 1");
		//	}
		//	catch (Exception ex)
		//	{
		//		Console.WriteLine($"Demo 1 failed: {ex.Message}");
		//		metrics.Notes += $"Failed: {ex.Message}";
		//		metrics.Display("Demo 1");
		//	}
		//	finally
		//	{
		//		await _consumerManager.DisposeAsync();
		//	}
		//}
		//public async Task RunDemo1()
		//{
		//	var metrics = new DemoMetrics();
		//	var stopwatch = System.Diagnostics.Stopwatch.StartNew();
		//	Console.WriteLine("=== Demo 1: Multiple Subscriptions on the Same Topic ===");

		//	try
		//	{
		//		// Create topic
		//		var topicData = new CreateTopicData(_serviceUrl, "OrderFifApr")
		//		{
		//			Tenant = "public",
		//			Namespace = "default",
		//			NumPartitions = 1
		//		};
		//		var topicResponse = await _topicManager.CreateTopic(topicData);
		//		Console.WriteLine(topicResponse.Message);
		//		metrics.Notes += $"Topic creation: {topicResponse.Message}; ";

		//		// Create consumers
		//		var subscriptions = new[] { "analytics-sub", "fraud-sub", "billing-sub" };
		//		var cts = new CancellationTokenSource();
		//		var messageCounts = new Dictionary<string, int>
		//		{
		//			["analytics-sub"] = 0,
		//			["fraud-sub"] = 0,
		//			["billing-sub"] = 0
		//		};

		//		async Task RunConsumer(string subName)
		//		{
		//			var consumerData = new ConsumerData
		//			{
		//				ServiceUrl = _serviceUrl,
		//				TopicName = "persistent://public/default/OrderFifApr",
		//				SubscriptionName = subName,
		//				SubscriptionType = SubscriptionType.Exclusive,
		//			};

		//			int localCount = 0; // Per-consumer counter, no shared state

		//			try
		//			{
		//				Console.WriteLine($"[{subName}] Connecting consumer...");
		//				_consumerManager.ConnectConsumer(consumerData, (c, ex) =>
		//					Console.WriteLine($"[ERROR] Consumer {subName}: {ex.Message}"));

		//				Console.WriteLine($"[{subName}] Subscribing...");
		//				await _consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
		//				{
		//					try
		//					{
		//						if (!string.IsNullOrEmpty(msg.ExceptionMessage))
		//						{
		//							Console.WriteLine($"[{subName}] Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
		//							return;
		//						}
		//						if (msg.MessageId == null)
		//						{
		//							Console.WriteLine($"[{subName}] Warning: Null MessageId, skipping.");
		//							return;
		//						}
		//						if (string.IsNullOrEmpty(msg.Data))
		//						{
		//							Console.WriteLine($"[{subName}] Warning: Empty message data, skipping.");
		//							return;
		//						}
		//						Console.WriteLine($"[{subName}] Received: {msg.Data}");
		//						localCount++;
		//						await _consumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId });
		//						Console.WriteLine($"[{subName}] Acknowledged message.");
		//						if (localCount >= 10)
		//						{
		//							Console.WriteLine($"[{subName}] Received 10 messages, signaling completion.");
		//							messageCounts[subName] = localCount; // Store final count
		//							if (messageCounts.All(kv => kv.Value >= 10))
		//							{
		//								Console.WriteLine($"[{subName}] All subscriptions completed, canceling.");
		//								cts.Cancel();
		//							}
		//						}
		//					}
		//					catch (Exception ex)
		//					{
		//						Console.WriteLine($"[{subName}] Error processing message: {ex.Message}");
		//					}
		//				}, cts.Token);
		//				Console.WriteLine($"[{subName}] Subscription completed.");
		//			}
		//			catch (OperationCanceledException)
		//			{
		//				Console.WriteLine($"[{subName}] Subscription canceled.");
		//				messageCounts[subName] = localCount; // Ensure final count is stored
		//			}
		//			catch (Exception ex)
		//			{
		//				Console.WriteLine($"[{subName}] Subscription failed: {ex.Message}");
		//			}
		//		}

		//		// Start consumers
		//		var consumerTasks = subscriptions.Select(sub => Task.Run(() => RunConsumer(sub), cts.Token)).ToArray();
		//		await Task.Delay(1000, cts.Token); // Wait for subscriptions

		//		// Connect producer
		//		var producerData = new ProducerData
		//		{
		//			ServiceUrl = _serviceUrl,
		//			TopicName = "persistent://public/default/OrderFifApr",
		//			EnableBatching = false
		//		};
		//		await _producerManager.ConnectProducer(producerData);
		//		Console.WriteLine("Producer connected.");

		//		// Send 10 messages
		//		for (int i = 1; i <= 10; i++)
		//		{
		//			var message = $"{{ \"orderId\": {i}, \"amount\": {i * 100} }}";
		//			await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
		//			metrics.MessagesSent++;
		//			Console.WriteLine($"Sent message {i}: {message}");
		//			await Task.Delay(100);
		//		}

		//		// Wait for consumers
		//		try
		//		{
		//			await Task.WhenAll(consumerTasks);
		//		}
		//		catch (OperationCanceledException)
		//		{
		//			Console.WriteLine("[INFO] Consumer tasks canceled after processing.");
		//		}

		//		stopwatch.Stop();
		//		metrics.SetupTime = stopwatch.Elapsed;
		//		metrics.MessagesReceived = messageCounts.Values.Sum();
		//		metrics.Notes += $"Total messages received: {metrics.MessagesReceived}; " +
		//						 string.Join(", ", messageCounts.Select(kv => $"{kv.Key}: {kv.Value} messages"));
		//		metrics.Display("Demo 1");
		//	}
		//	catch (Exception ex)
		//	{
		//		Console.WriteLine($"Demo 1 failed: {ex.Message}");
		//		metrics.Notes += $"Failed: {ex.Message}";
		//		metrics.Display("Demo 1");
		//	}
		//	finally
		//	{
		//		await _consumerManager.DisposeAsync();
		//	}
		//}
		//public async Task RunDemo2()
		//{
		//	var metrics = new DemoMetrics();
		//	var stopwatch = System.Diagnostics.Stopwatch.StartNew();
		//	Console.WriteLine("=== Demo 2: Shared Subscription for Parallel Processing ===");

		//	try
		//	{
		//		// Create topic
		//		var topicData = new CreateTopicData(_serviceUrl, "Shared-demo2-test")
		//		{
		//			Tenant = "public",
		//			Namespace = "default",
		//			NumPartitions = 1
		//		};
		//		var topicResponse = await _topicManager.CreateTopic(topicData);
		//		Console.WriteLine(topicResponse.Message);
		//		metrics.Notes += $"Topic creation: {topicResponse.Message}; ";

		//		// Prepare
		//		var cts = new CancellationTokenSource();
		//		var messageCounts = new ConcurrentDictionary<int, int>();
		//		var readySignals = new Dictionary<int, TaskCompletionSource<bool>>();

		//		for (int i = 1; i <= 3; i++)
		//		{
		//			messageCounts[i] = 0;
		//			readySignals[i] = new TaskCompletionSource<bool>();
		//		}

		//		async Task RunConsumer(int consumerId)
		//		{
		//			var consumerData = new ConsumerData
		//			{
		//				ServiceUrl = _serviceUrl,
		//				TopicName = "persistent://public/default/Shared-demo2-test",
		//				SubscriptionName = "shared-sub",
		//				SubscriptionType = SubscriptionType.Shared
		//			};

		//			int localCount = 0;

		//			try
		//			{
		//				var isolatedConsumerManager = new ConsumerManager();
		//				isolatedConsumerManager.ConnectConsumer(consumerData, (c, ex) =>
		//					Console.WriteLine($"[ERROR] Consumer {consumerId}: {ex.Message}"));

		//				Console.WriteLine($"[Consumer-{consumerId}] Subscribing...");
		//				readySignals[consumerId].SetResult(true); // ✅ Signal ready

		//				await isolatedConsumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
		//				{
		//					try
		//					{
		//						if (!string.IsNullOrEmpty(msg.ExceptionMessage))
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Error message: {msg.ExceptionMessage}");
		//							return;
		//						}
		//						if (msg.MessageId == null || string.IsNullOrEmpty(msg.Data))
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Skipping invalid message.");
		//							return;
		//						}

		//						Console.WriteLine($"[Consumer-{consumerId}] Received: {msg.Data}");
		//						localCount++;
		//						messageCounts[consumerId] = localCount;

		//						msg.Consumer.Acknowledge(msg.MessageId);
		//						Console.WriteLine($"[Consumer-{consumerId}] Acknowledged message.");

		//						if (messageCounts.Values.Sum() >= 30)
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] All messages processed, canceling.");
		//							cts.Cancel();
		//						}
		//					}
		//					catch (Exception ex)
		//					{
		//						Console.WriteLine($"[Consumer-{consumerId}] Message error: {ex.Message}");
		//					}
		//				}, cts.Token);

		//				Console.WriteLine($"[Consumer-{consumerId}] Subscription completed.");
		//			}
		//			catch (OperationCanceledException)
		//			{
		//				Console.WriteLine($"[INFO] Subscription to topic '{consumerData.TopicName}' with '{consumerData.SubscriptionName}' was canceled as expected.");
		//			}
		//			catch (Exception ex)
		//			{
		//				Console.WriteLine($"[Consumer-{consumerId}] Subscription failed: {ex.Message}");
		//			}
		//		}

		//		// Start consumers first
		//		var consumerTasks = Enumerable.Range(1, 3)
		//			.Select(id => Task.Run(() => RunConsumer(id), cts.Token))
		//			.ToArray();

		//		// ✅ Wait for all consumers to subscribe
		//		await Task.WhenAll(readySignals.Values.Select(tcs => tcs.Task));
		//		Console.WriteLine("[MAIN] All consumers are ready. Starting publishing...");

		//		// Connect producer
		//		var producerData = new ProducerData
		//		{
		//			ServiceUrl = _serviceUrl,
		//			TopicName = "persistent://public/default/Shared-demo2-test",
		//			EnableBatching = false
		//		};
		//		await _producerManager.ConnectProducer(producerData);
		//		Console.WriteLine("Producer connected.");

		//		// Send 30 messages
		//		for (int i = 1; i <= 30; i++)
		//		{
		//			var message = $"{{ \"taskId\": {i}, \"type\": \"process\" }}";
		//			await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
		//			metrics.MessagesSent++;
		//			Console.WriteLine($"Sent message {i}: {message}");
		//			await Task.Delay(50);
		//		}

		//		// Wait for consumers to finish
		//		try
		//		{
		//			await Task.WhenAll(consumerTasks);
		//		}
		//		catch (OperationCanceledException)
		//		{
		//			Console.WriteLine("[INFO] Consumer tasks were canceled after processing.");
		//		}

		//		stopwatch.Stop();
		//		metrics.SetupTime = stopwatch.Elapsed;
		//		metrics.MessagesReceived = messageCounts.Values.Sum();
		//		metrics.Notes += " Shared subscription message distribution; " +
		//			string.Join(", ", messageCounts.Select(kv => $"Consumer-{kv.Key}: {kv.Value} msgs"));
		//		metrics.Display("Demo 2");
		//	}
		//	catch (Exception ex)
		//	{
		//		Console.WriteLine($"Demo 2 failed: {ex.Message}");
		//		metrics.Notes = $"Failed: {ex.Message}";
		//		metrics.Display("Demo 2");
		//	}
		//}
		//public async Task RunDemo2()
		//{
		//working
		//	var metrics = new DemoMetrics();
		//	var stopwatch = System.Diagnostics.Stopwatch.StartNew();
		//	Console.WriteLine("=== Demo 2: Shared Subscription for Parallel Processing ===");

		//	List<ConsumerManager> consumerManagers = new();
		//	try
		//	{
		//		// Create topic
		//		var topicData = new CreateTopicData(_serviceUrl, "Shared-demo2-test2")
		//		{
		//			Tenant = "public",
		//			Namespace = "default",
		//			NumPartitions = 1
		//		};
		//		var topicResponse = await _topicManager.CreateTopic(topicData);
		//		Console.WriteLine(topicResponse.Message);
		//		metrics.Notes += $"Topic creation: {topicResponse.Message}; ";

		//		// Prepare
		//		var cts = new CancellationTokenSource();
		//		var messageCounts = new ConcurrentDictionary<int, int>();
		//		var readySignals = new Dictionary<int, TaskCompletionSource<bool>>();

		//		for (int i = 1; i <= 3; i++)
		//		{
		//			messageCounts[i] = 0;
		//			readySignals[i] = new TaskCompletionSource<bool>();
		//		}

		//		async Task RunConsumer(int consumerId)
		//		{
		//			var consumerManager = new ConsumerManager(
		//				errorHandler: (c, ex) => Console.WriteLine($"[ERROR] Consumer {consumerId}: {ex.Message}"),
		//				subscriptionType: SubscriptionType.Shared,
		//				messagePrefetchCount: 1 // Low prefetch for fair distribution
		//			);
		//			consumerManagers.Add(consumerManager);

		//			var consumerData = new ConsumerData
		//			{
		//				ServiceUrl = _serviceUrl,
		//				TopicName = "persistent://public/default/Shared-demo2-test2",
		//				SubscriptionName = "shared-sub",
		//				SubscriptionType = SubscriptionType.Shared
		//			};

		//			try
		//			{
		//				Console.WriteLine($"[Consumer-{consumerId}] Connecting...");
		//				consumerManager.ConnectConsumer(consumerData, (c, ex) =>
		//					Console.WriteLine($"[ERROR] Consumer {consumerId}: {ex.Message}"));

		//				Console.WriteLine($"[Consumer-{consumerId}] Subscribing...");
		//				readySignals[consumerId].SetResult(true);

		//				await consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
		//				{
		//					try
		//					{
		//						if (!string.IsNullOrEmpty(msg.ExceptionMessage))
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
		//							return;
		//						}
		//						if (msg.MessageId == null)
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Warning: Null MessageId, skipping.");
		//							return;
		//						}
		//						if (string.IsNullOrEmpty(msg.Data))
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Warning: Empty message data, skipping.");
		//							return;
		//						}

		//						Console.WriteLine($"[Consumer-{consumerId}] Received: {msg.Data}");
		//						messageCounts.AddOrUpdate(consumerId, 1, (k, v) => v + 1);

		//						await consumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId });
		//						Console.WriteLine($"[Consumer-{consumerId}] Acknowledged message.");

		//						// Simulate processing to allow distribution
		//						await Task.Delay(100);

		//						if (messageCounts.Values.Sum() >= 30)
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] All messages processed, canceling.");
		//							cts.Cancel();
		//						}
		//					}
		//					catch (Exception ex)
		//					{
		//						Console.WriteLine($"[Consumer-{consumerId}] Error processing message: {ex.Message}");
		//					}
		//				}, cts.Token);

		//				Console.WriteLine($"[Consumer-{consumerId}] Subscription completed.");
		//			}
		//			catch (OperationCanceledException)
		//			{
		//				Console.WriteLine($"[Consumer-{consumerId}] Subscription canceled.");
		//			}
		//			catch (Exception ex)
		//			{
		//				Console.WriteLine($"[Consumer-{consumerId}] Subscription failed: {ex.Message}");
		//			}
		//		}

		//		// Start consumers
		//		var consumerTasks = Enumerable.Range(1, 3)
		//			.Select(id => Task.Run(() => RunConsumer(id), cts.Token))
		//			.ToArray();

		//		// Wait for all consumers to subscribe
		//		await Task.WhenAll(readySignals.Values.Select(tcs => tcs.Task));
		//		Console.WriteLine("[MAIN] All consumers are ready. Starting publishing...");

		//		// Connect producer
		//		var producerData = new ProducerData
		//		{
		//			ServiceUrl = _serviceUrl,
		//			TopicName = "persistent://public/default/Shared-demo2-test2",
		//			EnableBatching = false // Match your PublishAsync usage
		//		};
		//		await _producerManager.ConnectProducer(producerData);
		//		Console.WriteLine("Producer connected.");

		//		// Send 30 messages
		//		for (int i = 1; i <= 30; i++)
		//		{
		//			var message = $"{{ \"taskId\": {i}, \"type\": \"process\" }}";
		//			await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
		//			metrics.MessagesSent++;
		//			Console.WriteLine($"Sent message {i}: {message}");
		//			await Task.Delay(50);
		//		}

		//		// Wait for consumers with timeout
		//		try
		//		{
		//			await Task.WhenAny(
		//				Task.WhenAll(consumerTasks),
		//				Task.Delay(TimeSpan.FromSeconds(30), cts.Token)
		//			);
		//			if (!cts.IsCancellationRequested)
		//			{
		//				Console.WriteLine("[WARNING] Timeout waiting for consumers, canceling.");
		//				cts.Cancel();
		//			}
		//			await Task.WhenAll(consumerTasks);
		//		}
		//		catch (OperationCanceledException)
		//		{
		//			Console.WriteLine("[INFO] Consumer tasks canceled after processing.");
		//		}
		//		catch (Exception ex)
		//		{
		//			Console.WriteLine($"[ERROR] Consumer tasks failed: {ex.Message}");
		//		}

		//		stopwatch.Stop();
		//		metrics.SetupTime = stopwatch.Elapsed;
		//		metrics.MessagesReceived = messageCounts.Values.Sum();
		//		metrics.Notes += "Shared subscription message distribution; " +
		//						 string.Join(", ", messageCounts.OrderBy(kv => kv.Key).Select(kv => $"Consumer-{kv.Key}: {kv.Value} msgs"));
		//		metrics.Display("Demo 2");
		//	}
		//	catch (Exception ex)
		//	{
		//		Console.WriteLine($"Demo 2 failed: {ex.Message}");
		//		metrics.Notes += $"Failed: {ex.Message}";
		//		metrics.Display("Demo 2");
		//	}
		//	finally
		//	{
		//		foreach (var manager in consumerManagers)
		//		{
		//			await manager.DisposeAsync();
		//		}
		//		_producerManager.Dispose(); // Changed to synchronous Dispose
		//		Console.WriteLine("[INFO] Resources disposed.");
		//	}
		//}

		//public async Task RunDemo2()
		//{
		//	var metrics = new DemoMetrics();
		//	var stopwatch = System.Diagnostics.Stopwatch.StartNew();
		//	Console.WriteLine("=== Demo 2: Shared Subscription for Parallel Processing ===");

		//	List<ConsumerManager> consumerManagers = new();
		//	try
		//	{
		//		// Create topic
		//		var topicData = new CreateTopicData(_serviceUrl, "Shared-demo2-test2")
		//		{
		//			Tenant = "public",
		//			Namespace = "default",
		//			NumPartitions = 1
		//		};
		//		var topicResponse = await _topicManager.CreateTopic(topicData);
		//		if (!topicResponse.IsSuccess)
		//		{
		//			throw new Exception($"Topic creation failed: {topicResponse.Message}");
		//		}
		//		Console.WriteLine(topicResponse.Message);
		//		metrics.Notes += $"Topic creation: {topicResponse.Message}; ";

		//		// Prepare
		//		var cts = new CancellationTokenSource();
		//		var messageCounts = new ConcurrentDictionary<int, int>();
		//		var readySignals = new Dictionary<int, TaskCompletionSource<bool>>();
		//		var cancellationLock = new object();
		//		bool cancellationTriggered = false;

		//		for (int i = 1; i <= 3; i++)
		//		{
		//			messageCounts[i] = 0;
		//			readySignals[i] = new TaskCompletionSource<bool>();
		//		}

		//		async Task RunConsumer(int consumerId)
		//		{
		//			var consumerManager = new ConsumerManager(
		//				errorHandler: (c, ex) => Console.WriteLine($"[ERROR] Consumer {consumerId}: {ex.Message}"),
		//				subscriptionType: SubscriptionType.Shared,
		//				messagePrefetchCount: 1
		//			);
		//			consumerManagers.Add(consumerManager);

		//			var consumerData = new ConsumerData
		//			{
		//				ServiceUrl = _serviceUrl,
		//				TopicName = "persistent://public/default/Shared-demo2-test2",
		//				SubscriptionName = "shared-sub",
		//				SubscriptionType = SubscriptionType.Shared
		//			};

		//			try
		//			{
		//				Console.WriteLine($"[Consumer-{consumerId}] Connecting...");
		//				consumerManager.ConnectConsumer(consumerData, (c, ex) =>
		//					Console.WriteLine($"[ERROR] Consumer {consumerId}: {ex.Message}"));

		//				await Task.Delay(100); // Stabilize connection

		//				Console.WriteLine($"[Consumer-{consumerId}] Subscribing...");
		//				readySignals[consumerId].SetResult(true);

		//				await consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
		//				{
		//					try
		//					{
		//						if (!string.IsNullOrEmpty(msg.ExceptionMessage))
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
		//							return;
		//						}
		//						if (msg.MessageId == null)
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Warning: Null MessageId, skipping.");
		//							return;
		//						}
		//						if (string.IsNullOrEmpty(msg.Data))
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Warning: Empty message data, skipping.");
		//							return;
		//						}

		//						// Check total messages to avoid processing extras
		//						if (messageCounts.Values.Sum() >= 30)
		//						{
		//							return;
		//						}

		//						Console.WriteLine($"[Consumer-{consumerId}] Received: {msg.Data}");
		//						messageCounts.AddOrUpdate(consumerId, 1, (k, v) => v + 1);

		//						// Retry acknowledgment
		//						bool acknowledged = false;
		//						for (int attempt = 1; attempt <= 3; attempt++)
		//						{
		//							try
		//							{
		//								await consumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId });
		//								acknowledged = true;
		//								Console.WriteLine($"[Consumer-{consumerId}] Acknowledged message.");
		//								break;
		//							}
		//							catch (Exception ex)
		//							{
		//								Console.WriteLine($"[Consumer-{consumerId}] Ack attempt {attempt}/3 failed: {ex.Message}");
		//								if (attempt < 3)
		//									await Task.Delay(50);
		//							}
		//						}

		//						if (!acknowledged)
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Failed to acknowledge message after 3 attempts.");
		//						}

		//						await Task.Delay(50); // Balance distribution and speed

		//						// Single cancellation at exactly 30 messages
		//						if (messageCounts.Values.Sum() == 30)
		//						{
		//							lock (cancellationLock)
		//							{
		//								if (!cancellationTriggered)
		//								{
		//									cancellationTriggered = true;
		//									Console.WriteLine($"[Consumer-{consumerId}] Exactly 30 messages processed, canceling.");
		//									cts.Cancel();
		//								}
		//							}
		//						}
		//					}
		//					catch (Exception ex)
		//					{
		//						Console.WriteLine($"[Consumer-{consumerId}] Error processing message: {ex.Message}");
		//					}
		//				}, cts.Token);

		//				Console.WriteLine($"[Consumer-{consumerId}] Subscription completed.");
		//			}
		//			catch (OperationCanceledException)
		//			{
		//				Console.WriteLine($"[Consumer-{consumerId}] Subscription canceled.");
		//			}
		//			catch (Exception ex)
		//			{
		//				Console.WriteLine($"[Consumer-{consumerId}] Subscription failed: {ex.Message}");
		//			}
		//		}

		//		// Start consumers
		//		var consumerTasks = Enumerable.Range(1, 3)
		//			.Select(id => Task.Run(() => RunConsumer(id), cts.Token))
		//			.ToArray();

		//		// Wait for all consumers to subscribe
		//		await Task.WhenAll(readySignals.Values.Select(tcs => tcs.Task));
		//		Console.WriteLine("[MAIN] All consumers are ready. Starting publishing...");

		//		// Connect producer
		//		var producerData = new ProducerData
		//		{
		//			ServiceUrl = _serviceUrl,
		//			TopicName = "persistent://public/default/Shared-demo2-test2",
		//			EnableBatching = false
		//		};
		//		await _producerManager.ConnectProducer(producerData);
		//		Console.WriteLine("Producer connected.");

		//		// Send 30 messages
		//		for (int i = 1; i <= 30; i++)
		//		{
		//			var message = $"{{ \"taskId\": {i}, \"type\": \"process\" }}";
		//			await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
		//			metrics.MessagesSent++;
		//			Console.WriteLine($"Sent message {i}: {message}");
		//			await Task.Delay(50);
		//		}

		//		// Wait for consumers with timeout
		//		try
		//		{
		//			await Task.WhenAny(
		//				Task.WhenAll(consumerTasks),
		//				Task.Delay(TimeSpan.FromSeconds(30), cts.Token)
		//			);
		//			if (!cts.IsCancellationRequested)
		//			{
		//				Console.WriteLine("[WARNING] Timeout waiting for consumers, canceling.");
		//				cts.Cancel();
		//			}
		//			await Task.WhenAll(consumerTasks);
		//		}
		//		catch (OperationCanceledException)
		//		{
		//			Console.WriteLine("[INFO] Consumer tasks canceled after processing.");
		//		}
		//		catch (Exception ex)
		//		{
		//			Console.WriteLine($"[ERROR] Consumer tasks failed: {ex.Message}");
		//		}

		//		stopwatch.Stop();
		//		metrics.SetupTime = stopwatch.Elapsed;
		//		metrics.MessagesReceived = messageCounts.Values.Sum();
		//		metrics.Notes += "Shared subscription message distribution; " +
		//						 string.Join(", ", messageCounts.OrderBy(kv => kv.Key).Select(kv => $"Consumer-{kv.Key}: {kv.Value} msgs"));
		//		metrics.Display("Demo 2");
		//	}
		//	catch (Exception ex)
		//	{
		//		Console.WriteLine($"Demo 2 failed: {ex.Message}");
		//		metrics.Notes += $"Failed: {ex.Message}";
		//		metrics.Display("Demo 2");
		//		return;
		//	}
		//	finally
		//	{
		//		foreach (var manager in consumerManagers)
		//		{
		//			await manager.DisposeAsync();
		//		}
		//		_producerManager.Dispose();
		//		Console.WriteLine("[INFO] Resources disposed.");
		//	}
		//}


		//public async Task RunDemo3()
		//{
		//	var metrics = new DemoMetrics();
		//	var stopwatch = System.Diagnostics.Stopwatch.StartNew();
		//	Console.WriteLine("=== Demo 3: Topic Hotspot Auto-Splitting ===");

		//	List<ConsumerManager> consumerManagers = new();
		//	try
		//	{
		//		// Create topic
		//		var topicData = new CreateTopicData(_serviceUrl, "Hotspot-demo3-test")
		//		{
		//			Tenant = "public",
		//			Namespace = "default",
		//			NumPartitions = 1
		//		};
		//		var topicResponse = await _topicManager.CreateTopic(topicData);
		//		if (!topicResponse.IsSuccess)
		//		{
		//			throw new Exception($"Topic creation failed: {topicResponse.Message}");
		//		}
		//		Console.WriteLine(topicResponse.Message);
		//		metrics.Notes += $"Topic creation: {topicResponse.Message}; ";

		//		// Prepare
		//		var cts = new CancellationTokenSource();
		//		var messageCounts = new ConcurrentDictionary<int, int>();
		//		var readySignals = new Dictionary<int, TaskCompletionSource<bool>>();

		//		for (int i = 1; i <= 2; i++)
		//		{
		//			messageCounts[i] = 0;
		//			readySignals[i] = new TaskCompletionSource<bool>();
		//		}

		//		async Task RunConsumer(int consumerId)
		//		{
		//			var consumerManager = new ConsumerManager(
		//				errorHandler: (c, ex) => Console.WriteLine($"[ERROR] Consumer {consumerId}: {ex.Message}"),
		//				subscriptionType: SubscriptionType.Shared,
		//				messagePrefetchCount: 1
		//			);
		//			consumerManagers.Add(consumerManager);

		//			var consumerData = new ConsumerData
		//			{
		//				ServiceUrl = _serviceUrl,
		//				TopicName = "persistent://public/default/Hotspot-demo3-test",
		//				SubscriptionName = "shared-sub",
		//				SubscriptionType = SubscriptionType.Shared
		//			};

		//			try
		//			{
		//				Console.WriteLine($"[Consumer-{consumerId}] Connecting...");
		//				consumerManager.ConnectConsumer(consumerData, (c, ex) =>
		//					Console.WriteLine($"[ERROR] Consumer {consumerId}: {ex.Message}"));

		//				await Task.Delay(1000); // Increased for stability
		//				Console.WriteLine($"[Consumer-{consumerId}] Subscribing...");
		//				readySignals[consumerId].SetResult(true);

		//				await consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
		//				{
		//					try
		//					{
		//						if (!string.IsNullOrEmpty(msg.ExceptionMessage))
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
		//							return;
		//						}
		//						if (msg.MessageId == null)
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Warning: Null MessageId, skipping.");
		//							return;
		//						}
		//						if (string.IsNullOrEmpty(msg.Data))
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Warning: Empty message data, skipping.");
		//							return;
		//						}

		//						messageCounts.AddOrUpdate(consumerId, 1, (k, v) => v + 1);
		//						Console.WriteLine($"[Consumer-{consumerId}] Received message {msg.Data}. Total: {messageCounts[consumerId]}");

		//						bool acknowledged = false;
		//						for (int attempt = 1; attempt <= 5; attempt++)
		//						{
		//							try
		//							{
		//								var response = await consumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId });
		//								if (response.IsSuccess)
		//								{
		//									acknowledged = true;
		//									Console.WriteLine($"[Consumer-{consumerId}] Acknowledged message.");
		//									break;
		//								}
		//								else
		//								{
		//									Console.WriteLine($"[Consumer-{consumerId}] Ack attempt {attempt}/5 failed: {response.Message}");
		//								}
		//							}
		//							catch (Exception ex)
		//							{
		//								Console.WriteLine($"[Consumer-{consumerId}] Ack attempt {attempt}/5 failed: {ex.Message}");
		//							}
		//							if (attempt < 5)
		//								await Task.Delay(300);
		//						}

		//						if (!acknowledged)
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] Failed to acknowledge after 5 attempts.");
		//						}

		//						await Task.Delay(10);

		//						if (messageCounts.Values.Sum() >= 20000)
		//						{
		//							Console.WriteLine($"[Consumer-{consumerId}] All messages processed, canceling.");
		//							cts.Cancel();
		//						}
		//					}
		//					catch (Exception ex)
		//					{
		//						Console.WriteLine($"[Consumer-{consumerId}] Error processing message: {ex.Message}");
		//					}
		//				}, cts.Token);

		//				Console.WriteLine($"[Consumer-{consumerId}] Subscription completed.");
		//			}
		//			catch (OperationCanceledException)
		//			{
		//				Console.WriteLine($"[Consumer-{consumerId}] Subscription canceled.");
		//			}
		//			catch (Exception ex)
		//			{
		//				Console.WriteLine($"[Consumer-{consumerId}] Subscription failed: {ex.Message}");
		//			}
		//		}

		//		// Start consumers
		//		var consumerTasks = Enumerable.Range(1, 2)
		//			.Select(id => Task.Run(() => RunConsumer(id), cts.Token))
		//			.ToArray();

		//		// Wait for consumers to subscribe
		//		await Task.WhenAll(readySignals.Values.Select(tcs => tcs.Task));
		//		Console.WriteLine("[MAIN] All consumers are ready. Starting publishing...");

		//		// Connect producer
		//		var producerData = new ProducerData
		//		{
		//			ServiceUrl = _serviceUrl,
		//			TopicName = "persistent://public/default/Hotspot-demo3-test",
		//			EnableBatching = false
		//		};
		//		await _producerManager.ConnectProducer(producerData);
		//		Console.WriteLine("Producer connected.");

		//		// Send 20,000 messages
		//		for (int i = 1; i <= 20000; i++)
		//		{
		//			var message = $"{{ \"taskId\": {i}, \"type\": \"process\" }}";
		//			await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
		//			metrics.MessagesSent++;
		//			if (i % 2000 == 0)
		//				Console.WriteLine($"Sent {i} messages...");
		//		}

		//		// Wait for consumers with timeout
		//		try
		//		{
		//			await Task.WhenAny(
		//				Task.WhenAll(consumerTasks),
		//				Task.Delay(TimeSpan.FromSeconds(120), cts.Token)
		//			);
		//			if (!cts.IsCancellationRequested)
		//			{
		//				Console.WriteLine("[WARNING] Timeout waiting for consumers, canceling.");
		//				cts.Cancel();
		//			}
		//			await Task.WhenAll(consumerTasks);
		//		}
		//		catch (OperationCanceledException)
		//		{
		//			Console.WriteLine("[INFO] Consumer tasks canceled after processing.");
		//		}
		//		catch (Exception ex)
		//		{
		//			Console.WriteLine($"[ERROR] Consumer tasks failed: {ex.Message}");
		//		}

		//		stopwatch.Stop();
		//		metrics.SetupTime = stopwatch.Elapsed;
		//		metrics.MessagesReceived = messageCounts.Values.Sum();
		//		metrics.Notes += "Hotspot auto-splitting demo; " +
		//						 string.Join(", ", messageCounts.OrderBy(kv => kv.Key).Select(kv => $"Consumer-{kv.Key}: {kv.Value} msgs")) +
		//						 "; Kafka requires manual repartitioning for scaling.";
		//		metrics.Display("Demo 3");
		//	}
		//	catch (Exception ex)
		//	{
		//		Console.WriteLine($"Demo 3 failed: {ex.Message}");
		//		metrics.Notes += $"Failed: {ex.Message}";
		//		metrics.Display("Demo 3");
		//		return;
		//	}
		//	finally
		//	{
		//		foreach (var manager in consumerManagers)
		//		{
		//			await manager.DisposeAsync();
		//		}
		//		_producerManager.Dispose();
		//		Console.WriteLine("[INFO] Resources disposed.");
		//	}
		//}
		//public async Task RunDemo4()
		//{
		//	var metrics = new DemoMetrics();
		//	var stopwatch = System.Diagnostics.Stopwatch.StartNew();
		//	Console.WriteLine("=== Demo 4: Per-Message Acknowledgment & Redelivery ===");

		//	// Create topic
		//	var topicData = new CreateTopicData(_serviceUrl, "ack-topic")
		//	{
		//		Tenant = "public",
		//		Namespace = "default",
		//		NumPartitions = 1
		//	};
		//	var topicResponse = await _topicManager.CreateTopic(topicData);
		//	Console.WriteLine(topicResponse.Message);

		//	// Connect producer
		//	var producerData = new ProducerData
		//	{
		//		ServiceUrl = _serviceUrl,
		//		TopicName = "persistent://public/default/ack-topic",
		//		EnableBatching = false
		//	};
		//	await _producerManager.ConnectProducer(producerData);

		//	// Send 10 messages
		//	for (int i = 1; i <= 10; i++)
		//	{
		//		var message = $"{{ \"msgId\": {i}, \"data\": \"test\" }}";
		//		await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
		//		metrics.MessagesSent++;
		//		await Task.Delay(100);
		//	}

		//	// Consumer with selective acknowledgment
		//	var consumerData = new ConsumerData
		//	{
		//		ServiceUrl = _serviceUrl,
		//		TopicName = "persistent://public/default/ack-topic",
		//		SubscriptionName = "ack-sub",
		//		SubscriptionType = SubscriptionType.Exclusive
		//	};
		//	var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
		//	var receivedMessages = new List<int>();
		//	var messageToSkip = 5; // Simulate failure on msgId 5
		//	MessageId skippedMessageId = null;

		//	_consumerManager.ConnectConsumer(consumerData, (c, ex) =>
		//		Console.WriteLine($"[ERROR] Consumer: {ex.Message}"));

		//	await _consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
		//	{
		//		var json = JsonDocument.Parse(msg.Data);
		//		var msgId = json.RootElement.GetProperty("msgId").GetInt32();
		//		Console.WriteLine($"[Ack-Consumer] Received: {msg.Data}");

		//		if (msgId == messageToSkip && skippedMessageId == null)
		//		{
		//			skippedMessageId = msg.MessageId!;
		//			Console.WriteLine($"[Ack-Consumer] Skipping ack for msgId {msgId}");
		//		}
		//		else
		//		{
		//			await _consumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId! });
		//			lock (receivedMessages) { receivedMessages.Add(1); }
		//		}
		//	}, cts.Token);

		//	// Redeliver skipped message
		//	if (skippedMessageId != null)
		//	{
		//		await Task.Delay(2000); // Wait to simulate processing
		//		Console.WriteLine($"[Ack-Consumer] Requesting redelivery for msgId {messageToSkip}");
		//		await _consumerManager.RedeliverUnacknowledgedMessagesAsync(new[] { skippedMessageId });
		//	}

		//	stopwatch.Stop();
		//	metrics.SetupTime = stopwatch.Elapsed;
		//	metrics.MessagesReceived = receivedMessages.Count;
		//	metrics.Notes = "Message 5 redelivered after selective non-acknowledgment.";
		//	metrics.Display("Demo 4");
		//}

		public async Task RunDemo5()
		{
			var metrics = new DemoMetrics();
			var stopwatch = System.Diagnostics.Stopwatch.StartNew();
			Console.WriteLine("=== Demo 5: Tiered Storage for Infinite Retention ===");

			// Create topic with retention
			var topicData = new CreateTopicData(_serviceUrl, "retention-topic")
			{
				Tenant = "public",
				Namespace = "default",
				NumPartitions = 1,
				RetentionMins = "10080" // 1 week
			};
			var topicResponse = await _topicManager.CreateTopic(topicData);
			Console.WriteLine(topicResponse.Message);

			// Connect producer
			var producerData = new ProducerData
			{
				ServiceUrl = _serviceUrl,
				TopicName = "persistent://public/default/retention-topic",
				EnableBatching = false
			};
			await _producerManager.ConnectProducer(producerData);

			// Send 10 messages
			for (int i = 1; i <= 10; i++)
			{
				var message = $"{{ \"eventId\": {i}, \"data\": \"persistent\" }}";
				await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
				metrics.MessagesSent++;
				await Task.Delay(100);
			}

			// Consumer to verify retention
			var consumerData = new ConsumerData
			{
				ServiceUrl = _serviceUrl,
				TopicName = "persistent://public/default/retention-topic",
				SubscriptionName = "retention-sub",
				SubscriptionType = SubscriptionType.Exclusive
			};
			var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
			var receivedMessages = new List<int>();

			_consumerManager.ConnectConsumer(consumerData, (c, ex) =>
				Console.WriteLine($"[ERROR] Consumer: {ex.Message}"));

			await _consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
			{
				Console.WriteLine($"[Retention-Consumer] Received: {msg.Data}");
				lock (receivedMessages) { receivedMessages.Add(1); }
				await _consumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId! });
			}, cts.Token);

			stopwatch.Stop();
			metrics.SetupTime = stopwatch.Elapsed;
			metrics.MessagesReceived = receivedMessages.Count;
			metrics.Notes = "Retention set to 1 week; S3 offload requires Pulsar config.";
			metrics.Display("Demo 5");
		}

		public async Task RunDemo6()
		{
			var metrics = new DemoMetrics();
			var stopwatch = System.Diagnostics.Stopwatch.StartNew();
			Console.WriteLine("=== Demo 6: Delayed Message Delivery ===");

			// Create topic
			var topicData = new CreateTopicData(_serviceUrl, "delayed-topic")
			{
				Tenant = "public",
				Namespace = "default",
				NumPartitions = 1
			};
			var topicResponse = await _topicManager.CreateTopic(topicData);
			Console.WriteLine(topicResponse.Message);

			// Connect producer
			var producerData = new ProducerData
			{
				ServiceUrl = _serviceUrl,
				TopicName = "persistent://public/default/delayed-topic",
				EnableBatching = false
			};
			await _producerManager.ConnectProducer(producerData);

			// Simulate delayed delivery (ProducerManager lacks DeliverAfter)
			for (int i = 1; i <= 5; i++)
			{
				var message = $"{{ \"eventId\": {i}, \"data\": \"delayed\", \"delaySeconds\": 5 }}";
				await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
				metrics.MessagesSent++;
				await Task.Delay(100);
				Console.WriteLine($"[Producer] Sent message {i} (intended 5s delay, requires DeliverAfter)");
			}

			// Consumer to verify delivery
			var consumerData = new ConsumerData
			{
				ServiceUrl = _serviceUrl,
				TopicName = "persistent://public/default/delayed-topic",
				SubscriptionName = "delayed-sub",
				SubscriptionType = SubscriptionType.Exclusive
			};
			var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
			var receivedMessages = new List<int>();

			_consumerManager.ConnectConsumer(consumerData, (c, ex) =>
				Console.WriteLine($"[ERROR] Consumer: {ex.Message}"));

			await _consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
			{
				Console.WriteLine($"[Delayed-Consumer] Received at {DateTime.UtcNow:HH:mm:ss}: {msg.Data}");
				lock (receivedMessages) { receivedMessages.Add(1); }
				await _consumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId! });
			}, cts.Token);

			stopwatch.Stop();
			metrics.SetupTime = stopwatch.Elapsed;
			metrics.MessagesReceived = receivedMessages.Count;
			metrics.Notes = "Delayed delivery requires DeliverAfter (not supported by ProducerManager).";
			metrics.Display("Demo 6");
		}

		public async Task RunDemo7()
		{
			var metrics = new DemoMetrics();
			var stopwatch = System.Diagnostics.Stopwatch.StartNew();
			Console.WriteLine("=== Demo 7: Dead Letter Queue Handling ===");

			// Create topic
			var topicData = new CreateTopicData(_serviceUrl, "dlq-topic")
			{
				Tenant = "public",
				Namespace = "default",
				NumPartitions = 1
			};
			var topicResponse = await _topicManager.CreateTopic(topicData);
			Console.WriteLine(topicResponse.Message);

			// Connect producer
			var producerData = new ProducerData
			{
				ServiceUrl = _serviceUrl,
				TopicName = "persistent://public/default/dlq-topic",
				EnableBatching = false
			};
			await _producerManager.ConnectProducer(producerData);

			// Send 10 messages
			for (int i = 1; i <= 10; i++)
			{
				var message = $"{{ \"msgId\": {i}, \"data\": \"test\" }}";
				await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
				metrics.MessagesSent++;
				await Task.Delay(100);
			}

			// Consumer with simulated DLQ behavior
			var consumerData = new ConsumerData
			{
				ServiceUrl = _serviceUrl,
				TopicName = "persistent://public/default/dlq-topic",
				SubscriptionName = "dlq-sub",
				SubscriptionType = SubscriptionType.Exclusive
			};
			var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
			var receivedMessages = new List<int>();

			_consumerManager.ConnectConsumer(consumerData, (c, ex) =>
				Console.WriteLine($"[ERROR] Consumer: {ex.Message}"));

			await _consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
			{
				var json = JsonDocument.Parse(msg.Data);
				var msgId = json.RootElement.GetProperty("msgId").GetInt32();
				Console.WriteLine($"[DLQ-Consumer] Received: {msg.Data}");

				if (msgId % 2 == 0) // Simulate failure for even msgIds
				{
					Console.WriteLine($"[DLQ-Consumer] Failing msgId {msgId} (would go to DLQ)");
				}
				else
				{
					await _consumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId! });
					lock (receivedMessages) { receivedMessages.Add(1); }
				}
			}, cts.Token);

			stopwatch.Stop();
			metrics.SetupTime = stopwatch.Elapsed;
			metrics.MessagesReceived = receivedMessages.Count;
			metrics.Notes = "Even msgIds simulated as DLQ candidates; requires DLQ policy config.";
			metrics.Display("Demo 7");
		}
	}
}



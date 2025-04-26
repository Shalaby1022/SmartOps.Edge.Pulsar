using SmartOps.Edge.Pulsar.BaseClasses.Contracts;
using SmartOps.Edge.Pulsar.BaseClasses.Models;
using SmartOps.Edge.Pulsar.Bus.Messages.Manager;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using DotPulsar;


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
		private readonly string _adminUrl = "http://localhost:8080";
		private const int TotalMessages = 1_000_000;
		private const int MessageSizeBytes = 2000; // Option: Set to 500 to exceed 100 MB/s
		private const int NumberOfProducers =10 ;

		public PulsarDemos(ITopicManager topicManager, IProducerManager producerManager, IConsumersManager consumerManager)
		{
			_topicManager = topicManager ?? throw new ArgumentNullException(nameof(topicManager));
			_producerManager = producerManager ?? throw new ArgumentNullException(nameof(producerManager));
			_consumerManager = consumerManager ?? throw new ArgumentNullException(nameof(consumerManager));
		}	
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
		public async Task RunDemo4()
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
	}
}



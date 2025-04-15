using DotPulsar;
using SmartOps.Edge.Pulsar.BaseClasses.Contracts;
using SmartOps.Edge.Pulsar.BaseClasses.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace PulsarDemoApp
{
	public class PulsarDemos
	{
		private readonly ITopicManager _topicManager;
		private readonly IProducerManager _producerManager;
		private readonly IConsumersManager _consumerManager;
		private readonly string _serviceUrl = "pulsar://localhost:6650";

		public PulsarDemos(ITopicManager topicManager, IProducerManager producerManager, IConsumersManager consumerManager)
		{
			_topicManager = topicManager ?? throw new ArgumentNullException(nameof(topicManager));
			_producerManager = producerManager ?? throw new ArgumentNullException(nameof(producerManager));
			_consumerManager = consumerManager ?? throw new ArgumentNullException(nameof(consumerManager));
		}
		//public async Task RunDemo1()
		//{
		//	var metrics = new DemoMetrics();
		//	var stopwatch = System.Diagnostics.Stopwatch.StartNew();
		//	Console.WriteLine("=== Demo 1: Single Subscription on a Topic ===");

		//	try
		//	{
		//		// Create topic
		//		var topicData = new CreateTopicData(_serviceUrl, "Order1")
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
		//			TopicName = "persistent://public/default/Order1",
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
		//				TopicName = "persistent://public/default/Order1",
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
		public async Task RunDemo1()
		{
			var metrics = new DemoMetrics();
			var stopwatch = System.Diagnostics.Stopwatch.StartNew();
			Console.WriteLine("=== Demo 1: Multiple Subscriptions on the Same Topic ===");

			try
			{
				// Create topic
				var topicData = new CreateTopicData(_serviceUrl, "OrderFifApr")
				{
					Tenant = "public",
					Namespace = "default",
					NumPartitions = 1
				};
				var topicResponse = await _topicManager.CreateTopic(topicData);
				Console.WriteLine(topicResponse.Message);
				metrics.Notes += $"Topic creation: {topicResponse.Message}; ";

				// Create consumers
				var subscriptions = new[] { "analytics-sub", "fraud-sub", "billing-sub" };
				var cts = new CancellationTokenSource();
				var messageCounts = new Dictionary<string, int>
				{
					["analytics-sub"] = 0,
					["fraud-sub"] = 0,
					["billing-sub"] = 0
				};

				async Task RunConsumer(string subName)
				{
					var consumerData = new ConsumerData
					{
						ServiceUrl = _serviceUrl,
						TopicName = "persistent://public/default/OrderFifApr",
						SubscriptionName = subName,
						SubscriptionType = SubscriptionType.Exclusive,
					};

					int localCount = 0; // Per-consumer counter, no shared state

					try
					{
						Console.WriteLine($"[{subName}] Connecting consumer...");
						_consumerManager.ConnectConsumer(consumerData, (c, ex) =>
							Console.WriteLine($"[ERROR] Consumer {subName}: {ex.Message}"));

						Console.WriteLine($"[{subName}] Subscribing...");
						await _consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
						{
							try
							{
								if (!string.IsNullOrEmpty(msg.ExceptionMessage))
								{
									Console.WriteLine($"[{subName}] Error: {msg.ExceptionMessage} (Code: {msg.ErrorCode})");
									return;
								}
								if (msg.MessageId == null)
								{
									Console.WriteLine($"[{subName}] Warning: Null MessageId, skipping.");
									return;
								}
								if (string.IsNullOrEmpty(msg.Data))
								{
									Console.WriteLine($"[{subName}] Warning: Empty message data, skipping.");
									return;
								}
								Console.WriteLine($"[{subName}] Received: {msg.Data}");
								localCount++;
								await _consumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId });
								Console.WriteLine($"[{subName}] Acknowledged message.");
								if (localCount >= 10)
								{
									Console.WriteLine($"[{subName}] Received 10 messages, signaling completion.");
									messageCounts[subName] = localCount; // Store final count
									if (messageCounts.All(kv => kv.Value >= 10))
									{
										Console.WriteLine($"[{subName}] All subscriptions completed, canceling.");
										cts.Cancel();
									}
								}
							}
							catch (Exception ex)
							{
								Console.WriteLine($"[{subName}] Error processing message: {ex.Message}");
							}
						}, cts.Token);
						Console.WriteLine($"[{subName}] Subscription completed.");
					}
					catch (OperationCanceledException)
					{
						Console.WriteLine($"[{subName}] Subscription canceled.");
						messageCounts[subName] = localCount; // Ensure final count is stored
					}
					catch (Exception ex)
					{
						Console.WriteLine($"[{subName}] Subscription failed: {ex.Message}");
					}
				}

				// Start consumers
				var consumerTasks = subscriptions.Select(sub => Task.Run(() => RunConsumer(sub), cts.Token)).ToArray();
				await Task.Delay(1000, cts.Token); // Wait for subscriptions

				// Connect producer
				var producerData = new ProducerData
				{
					ServiceUrl = _serviceUrl,
					TopicName = "persistent://public/default/OrderFifApr",
					EnableBatching = false
				};
				await _producerManager.ConnectProducer(producerData);
				Console.WriteLine("Producer connected.");

				// Send 10 messages
				for (int i = 1; i <= 10; i++)
				{
					var message = $"{{ \"orderId\": {i}, \"amount\": {i * 100} }}";
					await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
					metrics.MessagesSent++;
					Console.WriteLine($"Sent message {i}: {message}");
					await Task.Delay(100);
				}

				// Wait for consumers
				try
				{
					await Task.WhenAll(consumerTasks);
				}
				catch (OperationCanceledException)
				{
					Console.WriteLine("[INFO] Consumer tasks canceled after processing.");
				}

				stopwatch.Stop();
				metrics.SetupTime = stopwatch.Elapsed;
				metrics.MessagesReceived = messageCounts.Values.Sum();
				metrics.Notes += $"Total messages received: {metrics.MessagesReceived}; " +
								 string.Join(", ", messageCounts.Select(kv => $"{kv.Key}: {kv.Value} messages"));
				metrics.Display("Demo 1");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Demo 1 failed: {ex.Message}");
				metrics.Notes += $"Failed: {ex.Message}";
				metrics.Display("Demo 1");
			}
			finally
			{
				await _consumerManager.DisposeAsync();
			}
		}
		public async Task RunDemo2()
		{
			var metrics = new DemoMetrics();
			var stopwatch = System.Diagnostics.Stopwatch.StartNew();
			Console.WriteLine("=== Demo 2: Shared Subscription for Parallel Processing ===");

			try
			{
				// Create topic
				var topicData = new CreateTopicData(_serviceUrl, "Shared-demo2-test")
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
					TopicName = "persistent://public/default/Shared-demo2-test",
					EnableBatching = false
				};
				await _producerManager.ConnectProducer(producerData);
				Console.WriteLine("Producer connected.");

				// Send 30 messages
				for (int i = 1; i <= 30; i++)
				{
					var message = $"{{ \"taskId\": {i}, \"type\": \"process\" }}";
					await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
					metrics.MessagesSent++;
					Console.WriteLine($"Sent message {i}: {message}");
					await Task.Delay(50);
				}

				// Create three consumers with shared subscription
				var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90)); // 90s to avoid timeout
				var receivedMessages = new List<int>();

				async Task RunConsumer(int consumerId)
				{
					var consumerData = new ConsumerData
					{
						ServiceUrl = _serviceUrl,
						TopicName = "persistent://public/default/Shared-demo2-test",
						SubscriptionName = "shared-sub",
						SubscriptionType = SubscriptionType.Shared
					};

					try
					{
						_consumerManager.ConnectConsumer(consumerData, (c, ex) =>
							Console.WriteLine($"[ERROR] Consumer {consumerId}: {ex.Message}"));

						Console.WriteLine($"[Consumer-{consumerId}] Subscribing...");
						await _consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
						{
							try
							{
								// Skip error messages
								if (!string.IsNullOrEmpty(msg.ExceptionMessage))
								{
									Console.WriteLine($"[Consumer-{consumerId}] Error message received: {msg.ExceptionMessage}");
									return;
								}
								if (msg.MessageId == null)
								{
									Console.WriteLine($"[Consumer-{consumerId}] Warning: Null MessageId, skipping.");
									return;
								}
								if (string.IsNullOrEmpty(msg.Data))
								{
									Console.WriteLine($"[Consumer-{consumerId}] Warning: Empty message data, skipping.");
									return;
								}
								Console.WriteLine($"[Consumer-{consumerId}] Received: {msg.Data}");
								lock (receivedMessages) { receivedMessages.Add(1); }
								await _consumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId });
								Console.WriteLine($"[Consumer-{consumerId}] Acknowledged message.");
							}
							catch (Exception ex)
							{
								Console.WriteLine($"[Consumer-{consumerId}] Error processing message: {ex.Message}");
							}
						}, cts.Token);
						Console.WriteLine($"[Consumer-{consumerId}] Subscription completed.");
					}
					catch (OperationCanceledException)
					{
						Console.WriteLine($"[Consumer-{consumerId}] Subscription canceled due to timeout.");
					}
					catch (Exception ex)
					{
						Console.WriteLine($"[Consumer-{consumerId}] Subscription failed: {ex.Message}");
					}
				}

				await Task.WhenAll(Enumerable.Range(1, 3).Select(i => Task.Run(() => RunConsumer(i), cts.Token)));
				stopwatch.Stop();
				metrics.SetupTime = stopwatch.Elapsed;
				metrics.MessagesReceived = receivedMessages.Count;
				metrics.Notes = "Messages distributed across consumers in shared subscription.";
				metrics.Display("Demo 2");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Demo 2 failed: {ex.Message}");
				metrics.Notes = $"Failed: {ex.Message}";
				metrics.Display("Demo 2");
			}
		}

		public async Task RunDemo3()
		{
			var metrics = new DemoMetrics();
			var stopwatch = System.Diagnostics.Stopwatch.StartNew();
			Console.WriteLine("=== Demo 3: Topic Hotspot Auto-Splitting ===");

			// Create topic
			var topicData = new CreateTopicData(_serviceUrl, "hot-topic")
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
				TopicName = "persistent://public/default/hot-topic",
				EnableBatching = true,
				BatchingMaxMessages = 100,
				BatchingMaxDelayMs = 500
			};
			await _producerManager.ConnectProducer(producerData);

			// Send 1000 messages
			for (int i = 1; i <= 1000; i++)
			{
				var message = $"{{ \"eventId\": {i}, \"data\": \"heavy\" }}";
				await _producerManager.PublishAsync(producerData.TopicName, message, $"corr-{i}");
				metrics.MessagesSent++;
				if (i % 100 == 0) await Task.Delay(100); // Simulate bursts
			}

			// Consumer to verify delivery
			var consumerData = new ConsumerData
			{
				ServiceUrl = _serviceUrl,
				TopicName = "persistent://public/default/hot-topic",
				SubscriptionName = "hot-sub",
				SubscriptionType = SubscriptionType.Exclusive
			};
			var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
			var receivedMessages = new List<int>();

			_consumerManager.ConnectConsumer(consumerData, (c, ex) =>
				Console.WriteLine($"[ERROR] Consumer: {ex.Message}"));

			await _consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
			{
				Console.WriteLine($"[Hot-Consumer] Received: {msg.Data}");
				lock (receivedMessages) { receivedMessages.Add(1); }
				await _consumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId! });
			}, cts.Token);

			stopwatch.Stop();
			metrics.SetupTime = stopwatch.Elapsed;
			metrics.MessagesReceived = receivedMessages.Count;
			metrics.Notes = "High load sent; check Pulsar logs for bundle splitting (requires cluster config).";
			metrics.Display("Demo 3");
		}

		public async Task RunDemo4()
		{
			var metrics = new DemoMetrics();
			var stopwatch = System.Diagnostics.Stopwatch.StartNew();
			Console.WriteLine("=== Demo 4: Per-Message Acknowledgment & Redelivery ===");

			// Create topic
			var topicData = new CreateTopicData(_serviceUrl, "ack-topic")
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
				TopicName = "persistent://public/default/ack-topic",
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

			// Consumer with selective acknowledgment
			var consumerData = new ConsumerData
			{
				ServiceUrl = _serviceUrl,
				TopicName = "persistent://public/default/ack-topic",
				SubscriptionName = "ack-sub",
				SubscriptionType = SubscriptionType.Exclusive
			};
			var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
			var receivedMessages = new List<int>();
			var messageToSkip = 5; // Simulate failure on msgId 5
			MessageId skippedMessageId = null;

			_consumerManager.ConnectConsumer(consumerData, (c, ex) =>
				Console.WriteLine($"[ERROR] Consumer: {ex.Message}"));

			await _consumerManager.SubscribeAsync(consumerData.TopicName, async msg =>
			{
				var json = JsonDocument.Parse(msg.Data);
				var msgId = json.RootElement.GetProperty("msgId").GetInt32();
				Console.WriteLine($"[Ack-Consumer] Received: {msg.Data}");

				if (msgId == messageToSkip && skippedMessageId == null)
				{
					skippedMessageId = msg.MessageId!;
					Console.WriteLine($"[Ack-Consumer] Skipping ack for msgId {msgId}");
				}
				else
				{
					await _consumerManager.AcknowledgeAsync(new List<MessageId> { msg.MessageId! });
					lock (receivedMessages) { receivedMessages.Add(1); }
				}
			}, cts.Token);

			// Redeliver skipped message
			if (skippedMessageId != null)
			{
				await Task.Delay(2000); // Wait to simulate processing
				Console.WriteLine($"[Ack-Consumer] Requesting redelivery for msgId {messageToSkip}");
				await _consumerManager.RedeliverUnacknowledgedMessagesAsync(new[] { skippedMessageId });
			}

			stopwatch.Stop();
			metrics.SetupTime = stopwatch.Elapsed;
			metrics.MessagesReceived = receivedMessages.Count;
			metrics.Notes = "Message 5 redelivered after selective non-acknowledgment.";
			metrics.Display("Demo 4");
		}

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



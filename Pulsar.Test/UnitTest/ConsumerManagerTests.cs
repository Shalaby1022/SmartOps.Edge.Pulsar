namespace Pulsar.Test.UnitTest
{
	using DotPulsar;
	using DotPulsar.Abstractions;
	using NSubstitute;
	using SmartOps.Edge.Pulsar.Messages.Manager;
	using SmartOps.Edge.Pulsar.BaseClasses.Models;
	using System;
	using System.Threading;
	using System.Threading.Tasks;
	using Xunit;

	public class ConsumerManagerTests
	{
		private readonly IPulsarClient _mockPulsarClient;

		public ConsumerManagerTests()
		{
			_mockPulsarClient = Substitute.For<IPulsarClient>();
		}

		private ConsumerData CreateValidConsumerData()
		{
			return new ConsumerData
			{
				ServiceUrl = "pulsar://localhost:6650",
				SubscriptionName = "test-subscription"
			};
		}

		[Fact]
		public void Constructor_InvalidPrefetchCount_ThrowsArgumentException()
		{
			var exception = Assert.Throws<ArgumentException>(() => new ConsumerManager(messagePrefetchCount: 0));
			Assert.Contains("Message prefetch count must be positive.", exception.Message);
		}

		[Fact]
		public void Constructor_ValidParameters_DoesNotThrow()
		{
			var manager = new ConsumerManager(messagePrefetchCount: 1000);
			Assert.NotNull(manager);
		}

		[Fact]
		public void ConnectConsumer_ValidData_Succeeds()
		{
			var consumerManager = new ConsumerManager(client: _mockPulsarClient);
			var consumerData = CreateValidConsumerData();
			Action<IConsumer<byte[]>, Exception> errorHandler = (consumer, ex) => { };
			consumerManager.ConnectConsumer(consumerData, errorHandler);
			Assert.NotNull(consumerManager);
		}

		[Fact]
		public void ConnectConsumer_NullData_ThrowsArgumentNullException()
		{
			var consumerManager = new ConsumerManager(client: _mockPulsarClient);
			Action<IConsumer<byte[]>, Exception> errorHandler = (consumer, ex) => { };
			var exception = Assert.Throws<ArgumentNullException>(() => consumerManager.ConnectConsumer(null, errorHandler));
			Assert.Contains("consumerData", exception.Message);
		}

		[Fact]
		public void ConnectConsumer_EmptySubscriptionName_ThrowsArgumentException()
		{
			var consumerManager = new ConsumerManager(client: _mockPulsarClient);
			var consumerData = new ConsumerData { ServiceUrl = "pulsar://localhost:6650", SubscriptionName = "" };
			Action<IConsumer<byte[]>, Exception> errorHandler = (consumer, ex) => { };
			var exception = Assert.Throws<ArgumentException>(() => consumerManager.ConnectConsumer(consumerData, errorHandler));
			Assert.Contains("SubscriptionName is required", exception.Message);
		}

		[Fact]
		public async Task SubscribeAsync_ClientNotConnected_ThrowsInvalidOperationException()
		{
			var consumerManager = new ConsumerManager(); // No client
			var callback = Substitute.For<Func<SubscribeMessage<string>, Task>>();
			var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
				consumerManager.SubscribeAsync("test-topic", callback, CancellationToken.None));
			Assert.Equal("Client not connected.", exception.Message);
		}

		[Fact]
		public async Task SubscribeAsync_EmptyTopic_ThrowsArgumentException()
		{
			var consumerManager = new ConsumerManager(client: _mockPulsarClient);
			var consumerData = CreateValidConsumerData();
			Action<IConsumer<byte[]>, Exception> errorHandler = (consumer, ex) => { };
			consumerManager.ConnectConsumer(consumerData, errorHandler); // Set _client

			var callback = Substitute.For<Func<SubscribeMessage<string>, Task>>();
			var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
				consumerManager.SubscribeAsync("", callback, CancellationToken.None));
			Assert.Contains("Topic is required", exception.Message);
		}
	}
}

using System.Collections.Concurrent;
using DotPulsar.Abstractions;
using DotPulsar;
using DotPulsar.Extensions;

namespace Pulsar.Test.UnitTest;

public class ProducerManagerTests
{
    private readonly ProducerManager _manager;
    private readonly string _topic;
    private readonly IProducer<byte[]> _mockProducer;
    private readonly IPulsarClient _mockClient;

    public ProducerManagerTests()
    {
        _manager = new ProducerManager();
        _topic = TestConstantValues.EXISTING_TOPIC;
        _mockProducer = Substitute.For<IProducer<byte[]>>();
        _mockClient = Substitute.For<IPulsarClient>();
    }

    [Fact]
    [Description("Check that ConnectProducer does not throw when called with valid config.")]
    public async Task ConnectProducer_ValidData_DoesNotThrow()
    {
        // Arrange
        var data = new ProducerData
        {
            ServiceUrl = TestConstantValues.BROKER_URL,
            TopicName = TestConstantValues.NEW_TOPIC
        };

        // Act
        var ex = await Record.ExceptionAsync(() => _manager.ConnectProducer(data));

        // Assert
        Assert.Null(ex);
    }

    [Fact]
    [Description("Check that a producer is stored after ConnectProducer is called.")]
    public async Task ConnectProducer_ValidData_StoresProducer()
    {
        // Arrange
        var data = new ProducerData
        {
            ServiceUrl = TestConstantValues.BROKER_URL,
            TopicName = TestConstantValues.NEW_TOPIC
        };

        // Act
        await _manager.ConnectProducer(data);

        // Assert
        var producers = ReflectionTestHelpers.GetPrivateField<ConcurrentDictionary<string, IProducer<byte[]>>>(_manager, "_producers");
        Assert.True(producers.ContainsKey(TestConstantValues.NEW_TOPIC));
    }

    [Fact]
    [Description("Check that PublishAsync sends message with correlationId.")]
    public async Task PublishAsync_WithCorrelationId_SendsMetadata()
    {
        // Arrange
        var message = "hello";
        var correlationId = "cid-123";

        _mockClient.CreateProducer(Arg.Any<ProducerOptions<byte[]>>()).Returns(_mockProducer);
        ReflectionTestHelpers.SetPrivateField(_manager, "_client", _mockClient);

        var dict = new ConcurrentDictionary<string, IProducer<byte[]>>();
        dict[_topic] = _mockProducer;
        ReflectionTestHelpers.SetPrivateField(_manager, "_producers", dict);

        // Act
        await _manager.PublishAsync(_topic, message, correlationId);

        // Assert
        await _mockProducer.Received(1).Send(
            Arg.Is<MessageMetadata>(meta => meta["correlationId"] == correlationId),
            Arg.Is<byte[]>(bytes => Encoding.UTF8.GetString(bytes) == message)
        );
    }

    [Fact]
    [Description("Check that PublishAsync sends message without correlationId.")]
    public async Task PublishAsync_WithoutCorrelationId_SendsRaw()
    {
        // Arrange
        var message = "no-correlation-id";

        _mockClient.CreateProducer(Arg.Any<ProducerOptions<byte[]>>()).Returns(_mockProducer);
        ReflectionTestHelpers.SetPrivateField(_manager, "_client", _mockClient);

        var dict = new ConcurrentDictionary<string, IProducer<byte[]>>();
        dict[_topic] = _mockProducer;
        ReflectionTestHelpers.SetPrivateField(_manager, "_producers", dict);

        // Act
        await _manager.PublishAsync(_topic, message);

        // Assert
        await _mockProducer.Received(1).Send(Arg.Is<byte[]>(b => Encoding.UTF8.GetString(b) == message));
    }

    [Fact]
    [Description("Check that Dispose disposes all producers and the client.")]
    public void Dispose_CleansUpResources()
    {
        // Arrange
        var dict = new ConcurrentDictionary<string, IProducer<byte[]>>();
        dict[_topic] = _mockProducer;

        ReflectionTestHelpers.SetPrivateField(_manager, "_client", _mockClient);
        ReflectionTestHelpers.SetPrivateField(_manager, "_producers", dict);

        // Act
        _manager.Dispose();

        // Assert
        _mockProducer.Received().DisposeAsync();
        _mockClient.Received().DisposeAsync();
    }

    [Fact]
    [Description("Check that calling Dispose twice does not throw.")]
    public void Dispose_CalledTwice_DoesNotThrow()
    {
        // Arrange
        ReflectionTestHelpers.SetPrivateField(_manager, "_client", _mockClient);

        // Act
        _manager.Dispose();
        var ex = Record.Exception(() => _manager.Dispose());

        // Assert
        Assert.Null(ex);
    }
        [Fact]
    [Description("Should throw when PublishAsync is called without connecting producer.")]
    public async Task PublishAsync_WithoutClient_Throws()
    {
        // Act
        var ex = await Record.ExceptionAsync(() =>
            _manager.PublishAsync("some-topic", "test"));

        // Assert
        Assert.IsType<InvalidOperationException>(ex);
        Assert.Contains("Call ConnectProducer first", ex!.Message);
    }

    [Fact]
    [Description("Should create new producer if topic not found in _producers dictionary.")]
    public async Task PublishAsync_TopicNotInProducers_CreatesProducer()
    {
        // Arrange
        var topic = "new-topic";
        var message = "Hello, world!";
        ReflectionTestHelpers.SetPrivateField(_manager, "_client", _mockClient);
        _mockClient.CreateProducer(Arg.Any<ProducerOptions<byte[]>>()).Returns(_mockProducer);

        var dict = new ConcurrentDictionary<string, IProducer<byte[]>>();
        ReflectionTestHelpers.SetPrivateField(_manager, "_producers", dict);

        // Act
        await _manager.PublishAsync(topic, message);

        // Assert
        Assert.True(dict.ContainsKey(topic));
        await _mockProducer.Received(1).Send(Arg.Is<byte[]>(b => Encoding.UTF8.GetString(b) == message));
    }

    [Fact]
    [Description("Should catch and log exception when Send throws during PublishAsync.")]
    public async Task PublishAsync_SendFails_CatchesAndLogsError()
    {
        // Arrange
        var topic = "fail-topic";
        var message = "Test message";

        var throwingProducer = Substitute.For<IProducer<byte[]>>();
        throwingProducer
            .When(p => p.Send(Arg.Any<byte[]>()))
            .Do(x => throw new Exception("Send failed"));

        var mockClient = Substitute.For<IPulsarClient>();
        mockClient.CreateProducer(Arg.Any<ProducerOptions<byte[]>>()).Returns(throwingProducer);

        var dict = new ConcurrentDictionary<string, IProducer<byte[]>>();
        dict[topic] = throwingProducer;

        ReflectionTestHelpers.SetPrivateField(_manager, "_client", mockClient);
        ReflectionTestHelpers.SetPrivateField(_manager, "_producers", dict);

        // Act
        var ex = await Record.ExceptionAsync(() => _manager.PublishAsync(topic, message));

        // Assert
        Assert.Null(ex); // Should not throw to caller
    }

    [Fact]
    [Description("Should send all messages successfully in batch without retries.")]
    public async Task PublishBatchAsync_AllMessagesSentSuccessfully()
    {
        // Arrange
        var topic = "batch-topic";
        var messages = new List<string> { "msg1", "msg2", "msg3" };

        _mockClient.CreateProducer(Arg.Any<ProducerOptions<byte[]>>()).Returns(_mockProducer);
        ReflectionTestHelpers.SetPrivateField(_manager, "_client", _mockClient);

        var dict = new ConcurrentDictionary<string, IProducer<byte[]>>();
        dict[topic] = _mockProducer;
        ReflectionTestHelpers.SetPrivateField(_manager, "_producers", dict);

        // Act
        await _manager.PublishBatchAsync(topic, messages);

        // Assert
        foreach (var msg in messages)
        {
            await _mockProducer.Received().Send(
                Arg.Any<MessageMetadata>(), 
                Arg.Is<byte[]>(b => Encoding.UTF8.GetString(b) == msg)
            );
        }
    }

    [Fact]
    [Description("Should retry sending message when Send fails initially.")]
    public async Task PublishBatchAsync_SendFailsThenSucceeds_Retries()
    {
        // Arrange
        var topic = "retry-topic";
        var messages = new List<string> { "retry-message" };
        int attempt = 0;

        var retryingProducer = Substitute.For<IProducer<byte[]>>();
        retryingProducer
            .Send(Arg.Any<MessageMetadata>(), Arg.Any<byte[]>())
            .Returns(callInfo =>
            {
                attempt++;
                if (attempt < 2)
                    return ValueTask.FromException<MessageId>(new Exception("Transient failure"));
                else
                    return new ValueTask<MessageId>(Substitute.For<MessageId>());
            });

        _mockClient.CreateProducer(Arg.Any<ProducerOptions<byte[]>>()).Returns(retryingProducer);
        ReflectionTestHelpers.SetPrivateField(_manager, "_client", _mockClient);

        var dict = new ConcurrentDictionary<string, IProducer<byte[]>>();
        dict[topic] = retryingProducer;
        ReflectionTestHelpers.SetPrivateField(_manager, "_producers", dict);

        // Act
        await _manager.PublishBatchAsync(topic, messages);

        // Assert
        await retryingProducer.Received().Send(Arg.Any<MessageMetadata>(), Arg.Any<byte[]>());
    }

    [Fact]
    [Description("Should log failure after exceeding max retries.")]
    public async Task PublishBatchAsync_ExceedsMaxRetries_LogsError()
    {
        // Arrange
        var topic = "unreliable-topic";
        var messages = new List<string> { "fail-me" };

        var unreliableProducer = Substitute.For<IProducer<byte[]>>();
        unreliableProducer
            .Send(Arg.Any<MessageMetadata>(), Arg.Any<byte[]>())
            .Returns(_ => ValueTask.FromException<MessageId>(new Exception("Permanent failure")));

        _mockClient.CreateProducer(Arg.Any<ProducerOptions<byte[]>>()).Returns(unreliableProducer);

        var dict = new ConcurrentDictionary<string, IProducer<byte[]>>();
        dict[topic] = unreliableProducer;
        ReflectionTestHelpers.SetPrivateField(_manager, "_client", _mockClient);
        ReflectionTestHelpers.SetPrivateField(_manager, "_producers", dict);

        // Act
        var ex = await Record.ExceptionAsync(() =>
            _manager.PublishBatchAsync(topic, messages, maxRetries: 2));

        // Assert
        Assert.Null(ex); // Should swallow exception, just logs it
        await unreliableProducer.Received(2).Send(Arg.Any<MessageMetadata>(), Arg.Any<byte[]>());
    }

    [Fact]
    [Description("Should send messages in batch with correlationId in metadata.")]
    public async Task PublishBatchAsync_WithCorrelationId_SetsMetadata()
    {
        // Arrange
        var topic = "metadata-batch";
        var messages = new List<string> { "msg1", "msg2" };
        var correlationId = "batch-cid-456";

        _mockClient.CreateProducer(Arg.Any<ProducerOptions<byte[]>>()).Returns(_mockProducer);

        var dict = new ConcurrentDictionary<string, IProducer<byte[]>>();
        dict[topic] = _mockProducer;
        ReflectionTestHelpers.SetPrivateField(_manager, "_client", _mockClient);
        ReflectionTestHelpers.SetPrivateField(_manager, "_producers", dict);

        // Act
        await _manager.PublishBatchAsync(topic, messages, correlationId);

        // Assert
        await _mockProducer.Received(2).Send(
            Arg.Is<MessageMetadata>(m => m["correlationId"] == correlationId),
            Arg.Any<byte[]>()
        );
    }
}

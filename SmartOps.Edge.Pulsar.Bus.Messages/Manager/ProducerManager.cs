using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using SmartOps.Edge.Pulsar.BaseClasses.Models;
using System.Collections.Concurrent;
using System.Text;

public class ProducerManager : IProducerManager
{
    private IPulsarClient? _client;
    private readonly ConcurrentDictionary<string, IProducer<byte[]>> _producers = new();
    private bool _disposed = false;

    public async Task ConnectProducer(ProducerData producerData)
    {
        try
        {
            _client = PulsarClient.Builder()
                .ServiceUrl(new Uri(producerData.ServiceUrl))
                .Build();

            if (!string.IsNullOrWhiteSpace(producerData.TopicName))
            {
                var producer = CreateProducerFromData(producerData);
                _producers[producerData.TopicName] = producer;
            }

            Console.WriteLine("[ConnectProducer] Successfully connected producer for topic '{0}'", producerData.TopicName);
            Console.Out.Flush();
            await Task.CompletedTask;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[{ConstantValues.TOPIC_CREATION_ERROR}] Failed to connect or create producer for topic '{producerData.TopicName}': {ex.Message}");
            Console.Error.Flush();
            throw;
        }
    }

    private IProducer<byte[]> CreateProducerFromData(ProducerData data)
    {
        if (_client == null)
            throw new InvalidOperationException("Pulsar client is not initialized.");

        var producerBuilder = _client.NewProducer(Schema.ByteArray)
            .Topic(data.TopicName);

        if (!string.IsNullOrWhiteSpace(data.ProducerName))
        {
            producerBuilder = producerBuilder.ProducerName(data.ProducerName);
        }

        return producerBuilder.Create();
    }

    public async Task PublishAsync(string topicName, string message, string? correlationId = null)
    {
        if (_client == null)
            throw new InvalidOperationException("Producer is not connected. Call ConnectProducer first.");

        try
        {
            var producer = _producers.GetOrAdd(topicName, t => CreateProducerFromData(new ProducerData
            {
                TopicName = t,
                EnableBatching = true,
                BatchingMaxMessages = 5000,
                BatchingMaxDelayMs = 5
            }));

            var data = Encoding.UTF8.GetBytes(message);
            if (!string.IsNullOrWhiteSpace(correlationId))
            {
                var metadata = new MessageMetadata { ["correlationId"] = correlationId };
                await producer.Send(metadata, data);
            }
            else
            {
                await producer.Send(data);
            }

            Console.WriteLine($"[PublishAsync] Message sent to topic '{topicName}'");
            Console.Out.Flush();
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[{ConstantValues.PRODUCE_MESSAGE_ERROR}] Failed to publish message to topic '{topicName}': {ex.Message}");
            Console.Error.Flush();
        }
    }

    public async Task PublishBatchAsync(string topicName, IEnumerable<string> messages, string? correlationId = null, int maxRetries = 3)
    {
        if (_client == null)
            throw new InvalidOperationException("Producer is not connected. Call ConnectProducer first.");

        var producer = _producers.GetOrAdd(topicName, t => CreateProducerFromData(new ProducerData
        {
            TopicName = t,
            EnableBatching = true,
            BatchingMaxMessages = 100,
            BatchingMaxDelayMs = 500
        }));

        var metadata = !string.IsNullOrWhiteSpace(correlationId) ? new MessageMetadata { ["correlationId"] = correlationId } : null;

        foreach (var msg in messages)
        {
            var data = Encoding.UTF8.GetBytes(msg);
            int attempt = 0;
            bool sent = false;

            while (!sent && attempt < maxRetries)
            {
                try
                {
                    if (metadata != null)
                    {
                        await producer.Send(metadata, data);
                    }
                    else
                    {
                        await producer.Send(data);
                    }
                    sent = true;
                    //Console.WriteLine($"[PublishBatchAsync] Sent message: {msg}");
                    Console.Out.Flush();
                }
                catch (Exception ex)
                {
                    attempt++;
                    Console.Error.WriteLine($"[PublishBatchAsync] Retry {attempt}/{maxRetries} failed for message '{msg}': {ex.Message}");
                    Console.Error.Flush();
                    if (attempt < maxRetries)
                    {
                        await Task.Delay(300);
                    }
                }
            }

            if (!sent)
            {
                Console.Error.WriteLine($"[PublishBatchAsync] Failed to send message after {maxRetries} attempts: {msg}");
                Console.Error.Flush();
            }
        }

        Console.WriteLine("[PublishBatchAsync] All messages processed.");
        Console.Out.Flush();
    }

    public void Dispose()
    {
        if (_disposed) return;

        foreach (var producer in _producers.Values)
        {
            producer.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        _client?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _disposed = true;
        Console.WriteLine("[Dispose] ProducerManager disposed.");
        Console.Out.Flush();
    }
}
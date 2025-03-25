using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using SmartOps.Edge.Pulsar.BaseClasses.Models;
using System.Collections.Concurrent;
using System.Text;

public class PulsarProducerManager : IProducerManager
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
                var options = new ProducerOptions<byte[]>(producerData.TopicName, Schema.ByteArray);

                if (!string.IsNullOrWhiteSpace(producerData.ProducerName))
                    options.ProducerName = producerData.ProducerName;

                var producer = _client.CreateProducer(options);
                _producers[producerData.TopicName] = producer;
            }

            await Task.CompletedTask;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[{ConstantValues.TOPIC_CREATION_ERROR}] Failed to connect or create producer for topic '{producerData.TopicName}': {ex.Message}");
            throw;
        }
    }

    public async Task PublishAsync(string topic, string message, string? correlationId = null)
    {
        if (_client == null)
            throw new InvalidOperationException("Producer is not connected. Call ConnectProducer first.");

        try
        {
            if (!_producers.TryGetValue(topic, out var producer))
            {
                var options = new ProducerOptions<byte[]>(topic, Schema.ByteArray);
                producer = _client.CreateProducer(options);
                _producers[topic] = producer;
            }

            var data = Encoding.UTF8.GetBytes(message);

            if (!string.IsNullOrWhiteSpace(correlationId))
            {
                var metadata = new MessageMetadata();
                metadata["correlationId"] = correlationId;
                await producer.Send(metadata, data);
            }
            else
            {
                await producer.Send(data);
            }

            Console.WriteLine($"[PublishAsync] Message sent to topic '{topic}'");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[{ConstantValues.PRODUCE_MESSAGE_ERROR}] Failed to publish message to topic '{topic}': {ex.Message}");
        }
    }

    public void Dispose()
    {
        if (_disposed) return;

        foreach (var producer in _producers.Values)
        {
            producer.DisposeAsync().AsTask().Wait();
        }

        _client?.DisposeAsync().AsTask().Wait();
        _disposed = true;
    }
}

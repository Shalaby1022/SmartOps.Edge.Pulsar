using SmartOps.Edge.Pulsar.BaseClasses.Models;

/// <summary>
/// Defines the contract for managing a Pulsar producer, including connecting and publishing messages.
/// </summary>
public interface IProducerManager : IDisposable
{
    /// <summary>
    /// Connects to a Pulsar broker using the provided configuration settings.
    /// </summary>
    /// <param name="config">Configuration parameters required to establish a connection.</param>
    /// <returns>A task representing the asynchronous connection operation.</returns>
    Task ConnectProducer(ProducerData config);

    /// <summary>
    /// Asynchronously sends a single message to the specified Pulsar topic.
    /// </summary>
    /// <param name="topicName">The name of the topic to which the message will be sent.</param>
    /// <param name="message">The message content as a string.</param>
    /// <param name="correlationId">Optional message key for routing or tracing.</param>
    /// <returns>A task representing the asynchronous publish operation.</returns>
    Task PublishAsync(string topicName, string message, string? correlationId = null);
    
    /// <summary>
    /// Asynchronously sends a batch of messages to the specified Pulsar topic with retry logic.
    /// </summary>
    /// <param name="topicName">The name of the topic to which the messages will be sent.</param>
    /// <param name="messages">A collection of message strings to be sent as a batch.</param>
    /// <param name="correlationId">Optional message key for routing or tracing. Applied to all messages in the batch.</param>
    /// <param name="maxRetries">The maximum number of retry attempts for each message in case of failure. Defaults to 3.</param>
    /// <returns>A task representing the asynchronous batch publish operation.</returns>
    Task PublishBatchAsync(string topicName, IEnumerable<string> messages, string? correlationId = null, int maxRetries = 3);

	  void Dispose();
}

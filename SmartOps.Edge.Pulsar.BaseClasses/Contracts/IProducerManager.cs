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
    /// <param name="topic">The name of the topic to which the message will be sent.</param>
    /// <param name="message">The message content as a string.</param>
    /// <param name="correlationId">Optional message key for routing or tracing.</param>
    /// <returns>A task representing the asynchronous publish operation.</returns>
    Task PublishAsync(string topic, string message, string? correlationId = null);
}

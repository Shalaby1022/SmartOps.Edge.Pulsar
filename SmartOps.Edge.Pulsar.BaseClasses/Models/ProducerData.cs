namespace SmartOps.Edge.Pulsar.BaseClasses.Models{
    ///<summary>
    ///Configuration settings for pulsar producer
    ///</summary>
    public class ProducerData{
            /// <summary>
    /// The Pulsar service URL (e.g., pulsar://localhost:6650).
    /// </summary>
    public string ServiceUrl { get; set; } = string.Empty;

    /// <summary>
    /// The name of the Pulsar topic to which messages will be sent.
    /// </summary>
    public string TopicName { get; set; } = string.Empty;

    /// <summary>
    /// An optional name for the producer. Useful for identifying producers in logs or monitoring tools.
    /// </summary>
    public string? ProducerName { get; set; }

    /// <summary>
    /// Timeout (in milliseconds) for sending messages. Defaults to 30000 ms.
    /// </summary>
    public int SendTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Enables batching of messages for higher throughput. Defaults to true.
    /// </summary>
    public bool EnableBatching { get; set; } = true;

    /// <summary>
    /// The maximum number of pending messages allowed in memory. Defaults to 1000.
    /// </summary>
    public int MaxPendingMessages { get; set; } = 1000;
    }
}

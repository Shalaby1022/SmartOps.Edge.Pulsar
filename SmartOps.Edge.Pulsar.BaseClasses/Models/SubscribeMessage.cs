using DotPulsar;

namespace SmartOps.Edge.Pulsar.BaseClasses.Models
{
	/// <summary>
	/// Represents a message consumed from a Pulsar topic or an error encountered during consumption, used to deliver data
	/// to the callback in <see cref="IConsumersManager.SubscribeAsync"/> for processing in Test Case 5 scenarios.
	/// </summary>
	/// <typeparam name="T">The type of the message data, typically <see cref="string"/> for UTF-8 decoded Pulsar messages.</typeparam>
	public class SubscribeMessage<T>
	{
		/// <summary>
		/// Initializes a new instance of <see cref="SubscribeMessage{T}"/> with empty exception details, preparing it to hold
		/// either a consumed message or an error from Test Case 5 consumption.
		/// </summary>
		public SubscribeMessage()
		{
			ExceptionMessage = string.Empty;
			ErrorCode = string.Empty;
		}

		/// <summary>
		/// Gets or sets the topic from which the message was consumed, such as "persistent://public/default/test-topic-1" in Test Case 5.
		/// </summary>
		public string? Topic { get; set; }

		/// <summary>
		/// Gets or sets the payload of the consumed message, such as "Message 1: Success", "Message 2: Retry", or "Message 3: Fail" in Test Case 5.
		/// </summary>
		public T? Data { get; set; }

		/// <summary>
		/// Gets or sets the unique identifier of the message, used for acknowledgment or redelivery operations in Test Case 5 processing.
		/// </summary>
		public MessageId? MessageId { get; set; } // Use DotPulsar.MessageId

		/// <summary>
		/// Gets or sets the Unix timestamp in milliseconds when the message was published, aiding in tracking message timing in Test Case 5 output.
		/// </summary>
		public long UnixTimeStampMs { get; set; }

		/// <summary>
		/// Gets or sets the exception message if an error occurs during consumption, empty when a message is successfully consumed in Test Case 5.
		/// </summary>
		public string ExceptionMessage { get; set; }

		/// <summary>
		/// Gets or sets the error code associated with a consumption failure, empty for successful messages like those in Test Case 5.
		/// </summary>
		public string ErrorCode { get; set; }
	}
}

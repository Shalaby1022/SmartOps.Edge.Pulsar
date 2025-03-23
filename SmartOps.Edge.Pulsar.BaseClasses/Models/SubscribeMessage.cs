using DotPulsar;

namespace SmartOps.Edge.Pulsar.BaseClasses.Models
{
	public class SubscribeMessage<T>
	{
		public SubscribeMessage()
		{
			ExceptionMessage = string.Empty;
			ErrorCode = string.Empty;
		}

		public string? Topic { get; set; }
		public T? Data { get; set; }
		public MessageId? MessageId { get; set; } // Use DotPulsar.MessageId
		public long UnixTimeStampMs { get; set; }
		public string ExceptionMessage { get; set; }
		public string ErrorCode { get; set; }
	}
}
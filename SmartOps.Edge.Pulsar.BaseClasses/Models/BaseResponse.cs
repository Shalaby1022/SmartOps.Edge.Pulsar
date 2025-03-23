namespace SmartOps.Edge.Pulsar.BaseClasses.Models
{
	/// <summary>
	/// Represents a standard response for Apache Pulsar operations.
	/// </summary>
	public class BaseResponse
	{
		/// <summary>
		/// Indicates whether the Pulsar operation was successful.
		/// </summary>
		public bool IsSuccess { get; set; } = false;

		/// <summary>
		/// A message describing the result of the Pulsar operation.
		/// </summary>
		public string Message { get; set; } = string.Empty;

		/// <summary>
		/// An optional error code identifying the failure reason, if applicable.
		/// </summary>
		public string ErrorCode { get; set; } = string.Empty;
	}
}
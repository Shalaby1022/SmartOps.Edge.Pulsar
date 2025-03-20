namespace SmartOps.Edge.Pulsar.BaseClasses.Models
{
	public class BaseResponse
	{
		public bool IsSuccess { get; set; } = false;
		public string Message { get; set; } = string.Empty;
		public string ErrorCode { get; set; } = string.Empty;
	}
}
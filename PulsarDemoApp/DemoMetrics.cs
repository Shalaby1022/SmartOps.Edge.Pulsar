using System;

namespace PulsarDemoApp
{
	public class DemoMetrics
	{
		public TimeSpan SetupTime { get; set; }
		public int MessagesSent { get; set; }
		public int MessagesReceived { get; set; }
		public string Notes { get; set; } = string.Empty;

		public void Display(string demoName)
		{
			Console.WriteLine($"--- {demoName} Metrics ---");
			Console.WriteLine($"Setup Time: {SetupTime.TotalSeconds:F2}s");
			Console.WriteLine($"Messages Sent: {MessagesSent}");
			Console.WriteLine($"Messages Received: {MessagesReceived}");
			Console.WriteLine($"Notes: {Notes}");
			Console.WriteLine("-------------------");
		}
	}
}
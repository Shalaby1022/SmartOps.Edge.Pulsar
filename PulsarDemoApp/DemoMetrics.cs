public class DemoMetrics
{
	public string DemoName { get; set; } = "Demo";
	public TimeSpan SetupTime { get; set; }
	public int MessagesSent { get; set; }
	public int MessagesReceived { get; set; }
	public string Notes { get; set; } = string.Empty;

	public void Display(string demoPrefix)
	{
		Console.WriteLine($"\n=== {demoPrefix} Metrics ===");
		Console.WriteLine($"Demo: {DemoName}");
		Console.WriteLine($"Setup Time: {SetupTime.TotalSeconds:F2} seconds");
		Console.WriteLine($"Messages Sent: {MessagesSent}");
		Console.WriteLine($"Messages Received: {MessagesReceived}");
		Console.WriteLine($"Notes: {Notes}");
		Console.WriteLine("====================\n");
	}
}
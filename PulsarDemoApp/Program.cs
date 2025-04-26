using Microsoft.Extensions.DependencyInjection;
using SmartOps.Edge.Pulsar.BaseClasses.Contracts;
using SmartOps.Edge.Pulsar.Bus.Messages.Manager;
using SmartOps.Edge.Pulsar.Messages.Manager;

namespace PulsarDemoApp
{
	class Program
	{
		static async Task Main(string[] args)
		{
			Console.WriteLine("=== Pulsar Demo App ===");

			// Set up dependency injection
			var services = new ServiceCollection();
			services.AddHttpClient();
			services.AddSingleton<ITopicManager, TopicManager>();
			services.AddSingleton<IProducerManager, ProducerManager>();
			services.AddSingleton<IConsumersManager, ConsumerManager>();
			services.AddSingleton<PulsarDemos>();
			var provider = services.BuildServiceProvider();

			// Run all demos
			var demos = provider.GetRequiredService<PulsarDemos>();
			await demos.RunDemo1();
			//await demos.RunDemo2();
			//await demos.RunDemo3();
			//await demos.RunDemo4();

			Console.WriteLine("=== All Pulsar Demos Complete ===");
		}
	}
}
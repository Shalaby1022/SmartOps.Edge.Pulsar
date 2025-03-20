using SmartOps.Edge.Pulsar.BaseClasses.Models;

namespace SmartOps.Edge.Pulsar.BaseClasses.Contracts
{
	public interface ITopicManager : IDisposable
	{
		Task<BaseResponse> CreateTopic(CreateTopicData createTopicData);
	}
}


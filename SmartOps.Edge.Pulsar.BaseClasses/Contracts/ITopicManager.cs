using SmartOps.Edge.Pulsar.BaseClasses.Models;

namespace SmartOps.Edge.Pulsar.BaseClasses.Contracts
{
	/// <summary>
	/// Defines a contract for managing Pulsar topics.
	/// </summary>
	public interface ITopicManager : IDisposable
	{
		/// <summary>
		/// Creates a new Pulsar topic based on the specified topic data.
		/// </summary>
		/// <param name="topicData">The data required to create the topic.</param>
		/// <returns>
		/// A task that represents the asynchronous operation.
		/// The task result contains a <see cref="BaseResponse"/> indicating the outcome.
		/// </returns>
		Task<BaseResponse> CreateTopic(CreateTopicData topicData);


	}
}


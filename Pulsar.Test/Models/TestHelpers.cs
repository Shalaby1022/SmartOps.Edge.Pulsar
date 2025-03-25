using DotPulsar.Abstractions;
using DotPulsar;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pulsar.Test.Models
{
	public static class TestHelpers
	{
		public static IMessage<byte[]> CreateMockMessage(string content, ulong publishTime)
		{
			var mockMessage = Substitute.For<IMessage<byte[]>>();
			mockMessage.Data.Returns(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(content)));
			mockMessage.MessageId.Returns(new MessageId(publishTime, publishTime, 1, 0));
			mockMessage.PublishTime.Returns(publishTime);
			mockMessage.ProducerName.Returns("test-producer");
			mockMessage.RedeliveryCount.Returns(0u);
			mockMessage.Properties.Returns(new Dictionary<string, string>());
			return mockMessage;
		}
	}
}

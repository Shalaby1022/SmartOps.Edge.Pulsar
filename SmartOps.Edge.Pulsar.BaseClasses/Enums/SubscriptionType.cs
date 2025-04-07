using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SmartOps.Edge.Pulsar.BaseClasses.Enums
{
	/// <summary>
	/// Defines the types of subscription modes for a message consumer.
	/// </summary>
	internal enum SubscriptionType
	{
		/// <summary>
		/// Only one consumer is allowed on the subscription. 
		/// Any additional consumers attempting to subscribe will be rejected.
		/// </summary>
		Exclusive,

		/// <summary>
		/// Multiple consumers can attach to the subscription. 
		/// Messages are distributed across consumers.
		/// </summary>
		Shared,

		/// <summary>
		/// A single active consumer with automatic failover to other consumers.
		/// </summary>
		Failover,

		/// <summary>
		/// Multiple consumers can attach, and messages are delivered based on a key.
		/// </summary>
		Key_Shared
	}

}

namespace Pulsar.Test.Models
{
	internal class ReflectionTestHelpers
	{
		public static Task InvokePrivateMethodAsync(object instance, string methodName, params object[] args)
		{
			var methodInfo = typeof(TopicManager).GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Instance);
			Assert.NotNull(methodInfo);
			try
			{
				return (Task)methodInfo.Invoke(instance, args);
			}
			catch (TargetInvocationException ex)
			{
				return Task.FromException(ex);
			}
		}

		public static T InvokeProtectedMethod<T>(object instance, string methodName, params object[] args)
		{
			var methodInfo = typeof(TopicManager).GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static);
			Assert.NotNull(methodInfo);
			return (T)methodInfo.Invoke(null, args);
		}
	}
}

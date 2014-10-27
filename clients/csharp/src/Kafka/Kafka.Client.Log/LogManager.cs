namespace Kafka.Client.Log
{
	using System;

	public static class LogManager
	{
		public static ILogger GetLogger(Type type)
		{
			return new Logger(log4net.LogManager.GetLogger(type));
		}

		public static ILogger GetLogger<T>()
		{
			return GetLogger(typeof(T));
		}
	}
}

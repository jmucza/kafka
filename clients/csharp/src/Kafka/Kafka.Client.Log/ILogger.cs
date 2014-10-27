namespace Kafka.Client.Log
{
	using System;
	using System.Globalization;

	/// <summary>
	/// Interface of logger
	/// </summary>
	/// <remarks>
	/// Introduced to decouple code from a specific logger (log4net)
	/// </remarks>
	public interface ILogger
	{
		void Debug(string message, Exception exception);

		void Debug(string message);

		void InfoFormat(CultureInfo currentCulture, string format, params object[] args);

		void Warn(string message, Exception exception);

		void Info(string message);

		void Error(string message, Exception exception);
	}
}
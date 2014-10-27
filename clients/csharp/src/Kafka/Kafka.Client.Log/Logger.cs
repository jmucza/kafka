namespace Kafka.Client.Log
{
	using System;
	using System.Globalization;

	using log4net;

	public class Logger : ILogger
	{
		private readonly ILog _log;

		public Logger(ILog log)
		{
			this._log = log;
		}

		public void Debug(string message, Exception exception)
		{
			this._log.Debug(message, exception);
		}

		public void Debug(string message)
		{
			this._log.Debug(message);
		}

		public void InfoFormat(CultureInfo currentCulture, string format, params object[] args)
		{
			this._log.InfoFormat(currentCulture, format, args);
		}

		public void Warn(string message, Exception exception)
		{
			this._log.Warn(message, exception);
		}

		public void Info(string message)
		{
			this._log.Info(message);
		}

		public void Error(string message, Exception exception)
		{
			this._log.Error(message, exception);
		}
	}
}
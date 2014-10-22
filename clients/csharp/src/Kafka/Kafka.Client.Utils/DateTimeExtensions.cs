using System;

namespace Kafka.Client.Utils
{
	public static class DateTimeExtensions
	{
		private static readonly DateTime Jan1st1970 = new DateTime
		(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

		/// <summary>
		/// gets the current time in milliseconds (equivalent of System.currentTimeMillis() in Java)
		/// </summary>
		/// <returns></returns>
		public static long CurrentTimeMillis()
		{
			return (long)(DateTime.UtcNow - Jan1st1970).TotalMilliseconds;
		}
	}
}

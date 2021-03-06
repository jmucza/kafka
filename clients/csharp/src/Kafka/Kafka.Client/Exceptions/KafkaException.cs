﻿/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Kafka.Client.Utils;

namespace Kafka.Client.Exceptions
{
    using System;

    /// <summary>
    /// A wrapping of an error code returned from Kafka.
    /// </summary>
    public class KafkaException : Exception
    {
        public KafkaException()
        {
            ErrorCode = ErrorMapping.NoError;
        }

        /// <summary>
        /// Initializes a new instance of the KafkaException class.
        /// </summary>
        /// <param name="errorCode">The error code generated by a request to Kafka.</param>
        public KafkaException(int errorCode) : base(GetMessage(errorCode))
        {
            ErrorCode = errorCode;
        }

        /// <summary>
        /// Gets the error code that was sent from Kafka.
        /// </summary>
        public int ErrorCode { get; private set; }

        /// <summary>
        /// Gets the message for the exception based on the Kafka error code.
        /// </summary>
        /// <param name="errorCode">The error code from Kafka.</param>
        /// <returns>A string message representation </returns>
        private static string GetMessage(int errorCode)
        {
            if (errorCode == ErrorMapping.OffsetOutOfRangeCode)
            {
                return "Offset out of range";
            }
            if (errorCode == ErrorMapping.InvalidMessageCode)
            {
                return "Invalid message";
            }
            if (errorCode == ErrorMapping.WrongPartitionCode)
            {
                return "Wrong partition";
            }
            if (errorCode == ErrorMapping.InvalidFetchSizeCode)
            {
                return "Invalid message size";
            }
            return "Unknown error";
        }
    }
}

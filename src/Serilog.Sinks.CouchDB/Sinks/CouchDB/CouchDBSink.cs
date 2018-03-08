// Copyright 2014 Serilog Contributors
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Formatting.Json;
using System.Net.Http.Headers;
using Serilog.Core;

namespace Serilog.Sinks.CouchDB
{
    /// <summary>
    /// Writes log events as documents to a CouchDB database.
    /// </summary>
    public class CouchDBSink : ILogEventSink, IDisposable
    {        
        volatile bool runEventQueueQueue = true;
        volatile bool processingLogEventQueue = false;
        string lastId = null;
        int batchPostingLimit;
        List<string> logEventQueue = new List<string>();
        Exception lastException = null;
        readonly IFormatProvider formatProvider;
        readonly HttpClient httpClient;
        const string BulkUploadResource = "_bulk_docs";
        TimeSpan period;

        /// <summary>
        /// A reasonable default for the number of events posted in
        /// each batch.
        /// </summary>
        public const int DefaultBatchPostingLimit = 50;

        /// <summary>
        /// A reasonable default time to wait between checking for event batches.
        /// </summary>
        public static readonly TimeSpan DefaultPeriod = TimeSpan.FromSeconds(2);

        /// <summary>
        /// Construct a sink posting to the specified database.
        /// </summary>
        /// <param name="databaseUrl">The URL of a CouchDB database.</param>
        /// <param name="batchPostingLimit">The maximum number of events to post in a single batch.</param>
        /// <param name="period">The time to wait between checking for event batches.</param>
        /// <param name="formatProvider">Supplies culture-specific formatting information, or null.</param>
        /// <param name="databaseUsername">The username to use in the HTTP Authentication header.</param>
        /// <param name="databasePassword">Password to use in the HTTP Authentication header</param>
        public CouchDBSink(string databaseUrl, int batchPostingLimit, TimeSpan period,
            IFormatProvider formatProvider, string databaseUsername, string databasePassword)
        {
            if (databaseUrl == null)
            {
                throw new ArgumentNullException("databaseUrl");
            }

            var baseAddress = databaseUrl;
            if (!databaseUrl.EndsWith("/"))
            {
                baseAddress += "/";
            }
            this.batchPostingLimit = batchPostingLimit;
            this.period = period;
            this.formatProvider = formatProvider;
            this.httpClient = new HttpClient { BaseAddress = new Uri(baseAddress) };

            if (databaseUsername != null & databasePassword != null)
            {
                var authByteArray = Encoding.ASCII.GetBytes(string.Format("{0}:{1}", databaseUsername, databasePassword));
                var authHeader = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(authByteArray));
                httpClient.DefaultRequestHeaders.Authorization = authHeader;
            }

            Task.Run(() =>
            {
                while (runEventQueueQueue)
                {
                    ProcessDocumentQueue();
                    Task.Delay((int)period.TotalMilliseconds).Wait();
                }
            });
        }


        /// <summary>
        /// Free resources held by the sink.
        /// </summary>
        /// <param name="disposing">If true, called because the object is being disposed; if false,
        /// the object is being disposed from the finalizer.</param>

        public void Dispose()
        {
            Task.Run(() =>
            {                
                while (logEventQueue.Count > 0 || processingLogEventQueue)
                {
                    Task.Delay(100).Wait();
                }
                runEventQueueQueue = false;
                Task.Delay(100).Wait();
            }).Wait();
            
             httpClient.Dispose();
        }

        /// <summary>
        /// Emit a batch of log events, running asynchronously.
        /// </summary>
        /// <param name="events">The events to emit.</param>
        /// <remarks>Override either <see cref="PeriodicBatchingSink.EmitBatch"/> or <see cref="PeriodicBatchingSink.EmitBatchAsync"/>,
        /// not both.</remarks>
        ///         
        public void Emit(LogEvent logEvent)
        {
            if (lastException != null)
            {
                throw lastException;
            }
            var payload = new StringWriter();
            var formatter = new JsonFormatter(renderMessage: true);
            formatter.Format(logEvent, payload);
            string ts = logEvent.Timestamp.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
            string id = ts + "10";
            int idx = 11;
            while (true)
            {
                if (!id.Equals(lastId))
                {
                    break;
                }
                id = ts + "" + idx;
                idx++;
            }
            lastId = id;

            string doc = payload.ToString().Replace("\"Timestamp\"", "\"_id\":\""+id+"\", \"Timestamp\"");

            lock(logEventQueue)
            {
                logEventQueue.Add(doc);
            }
        }

        private async void ProcessDocumentQueue()
        {
            if (processingLogEventQueue)
            {
                return;
            }
            processingLogEventQueue = true;
            List<string> tmpQueue = new List<string>();
            lock (logEventQueue)
            {
                if (logEventQueue.Count > 0)
                {
                    tmpQueue.AddRange(logEventQueue);
                    logEventQueue.Clear();
                }
            }
            if (tmpQueue.Count > 0)
            {
                var payload = new StringWriter();
                payload.Write("{\"docs\":[");

                var delim = "";
                foreach (var doc in tmpQueue)
                {
                    payload.Write(delim);
                    payload.Write(doc);                    
                    delim = ",";
                }
                payload.Write("]}");
             
                var content = new StringContent(payload.ToString(), Encoding.UTF8, "application/json");
                var result = await httpClient.PostAsync(BulkUploadResource, content);
                if (!result.IsSuccessStatusCode)
                {
                    lastException = new LoggingFailedException(string.Format("Received failed result {0} when posting events to CouchDB", result.StatusCode));
                }
            }
            processingLogEventQueue = false;
        }
    }
}

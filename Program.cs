using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace Confluent.Kafka.Examples.Rest
{
    class RestClient : IDisposable
    {
        private HttpClient client;

        public RestClient(string url)
        {
            client = new HttpClient();
            client.BaseAddress = new Uri(url);
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("*/*"));
        }

        public static void SetJsonContent(HttpRequestMessage request, JObject jsonContent)
        {
            request.Content = new ByteArrayContent(Encoding.UTF8.GetBytes(jsonContent.ToString(Formatting.None)));
            request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/vnd.kafka.json.v2+json");
        }

        public async Task<JArray> GetTopics()
        {
            var req = new HttpRequestMessage(HttpMethod.Get, "topics");
            var response = await client.SendAsync(req);
            return JArray.Parse(Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync()));
        }

        public async Task<JObject> GetTopicInfo(string topic)
        {
            var req = new HttpRequestMessage(HttpMethod.Get, $"topics/{topic}");
            var response = await client.SendAsync(req);
            return JObject.Parse(Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync()));
        }

        public async Task<JObject> ProduceJson(string topic, JObject json)
        {
            var req = new HttpRequestMessage(HttpMethod.Post, $"topics/{topic}");
            SetJsonContent(req, json);
            var response = await client.SendAsync(req);
            return JObject.Parse(Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync()));
        }

        public void Dispose()
        {
            client.Dispose();
        }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            using (var restClient = new RestClient("http://localhost:8082/"))
            {
                var topics = await restClient.GetTopics();
                Console.WriteLine(topics.ToString(Formatting.Indented));

                var topicInfo = await restClient.GetTopicInfo("rest-test");
                Console.WriteLine(topicInfo.ToString(Formatting.Indented));

                var produceResult = await restClient.ProduceJson(
                    "rest-test", JObject.FromObject(new { records = new [] { new { @value = new { foo = "bar" } } } }));
                Console.WriteLine(produceResult.ToString(Formatting.Indented));
            }
        }

    }
}



namespace TestApplication
{
    using Azure.DocumentDBRepository;
    using Microsoft.Azure.Documents;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    class Program
    {
        static void Main(string[] args)
        {
            string endpoint = "test";
            string authKey = "test";

            var repo = new DocumentDBRepository<TestDocument>(endpoint, authKey, "DataPointsReportsDatabase", "TestRepo", 2, u => ((TestDocument)u).Id,
                new Microsoft.Azure.Documents.Client.ConnectionPolicy()
                {
                    ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Direct,
                    ConnectionProtocol = Microsoft.Azure.Documents.Client.Protocol.Tcp
                });
            var colls = repo.PartitionCollectionsSelfLinks;

            Stopwatch sw = new Stopwatch();
            sw.Start();
            //var collections = repo.GetById("123");
            for (int i = 0; i < 5; i++)
            {
                var insert = new TestDocument()
                {
                    Id = Guid.NewGuid().ToString(),
                    Test = "erter" + i
                };

                var result = repo.InsertItemAsync(insert).Result;
                insert.Id = result.Id;
                insert.Test="1231123";
                var update = repo.UpdateAsync(insert);
                var delete = repo.DeleteAsync(result.Id).Result;

            }
            sw.Stop();
            var getresult = repo.Get(new SqlQuerySpec("Select * from c where c.test = '1231123'"));
        }
    }

    public class TestDocument : Document
    {
        [JsonProperty("test")]
        public string Test { get; set; }
    }
}

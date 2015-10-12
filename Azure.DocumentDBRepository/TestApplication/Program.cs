

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
            string endpoint = "htest";
            string authKey = "test";
            var connectionPolicy = new Microsoft.Azure.Documents.Client.ConnectionPolicy()
                    {
                        ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Direct,
                        ConnectionProtocol = Microsoft.Azure.Documents.Client.Protocol.Tcp
                    };

            var repo = new DocumentDBRepository<TestDocument>(endpoint, authKey, 
                "DataPointsReportsDatabase",
                "TestRepo", 2, u => ((TestDocument)u).PartitionKey,
                    connectionPolicy);

            Console.WriteLine("Repository created: 2 collections used.");

            // Keep the self links in memory to be able to delete them in the end.
            List<string> recordSelfLinks = new List<string>();

            for (int i = 0; i < 50; i++)
            {
                var insert = new TestDocument()
                {
                    TestProp = "testDoc" + i,
                    PartitionKey = (i % 4).ToString()
                };

                recordSelfLinks.Add( repo.InsertItemAsync(insert).Result.SelfLink);
            }
            Console.WriteLine("50 records added with 4 partition keys.");

            // Get all records from the collections where partition keys 0 and 1 are assigned to.
            var allResults = repo.Get(x => (x.PartitionKey == "0" || x.PartitionKey == "1"));
            Console.WriteLine(string.Format("Number of records in collections with partitions 0 and 1: {0}", allResults.Count()));

            // We need to pass a predicate as well.
            // Otherwise we will get all records from the collection where
            // partitions with a value of "0" are assigned to by the partitioner.
            var allRecordsByFirstPartition = repo.Get(new TestDocument() { PartitionKey = "0" }, 
                x=> x.PartitionKey == "0").ToList();
            Console.WriteLine(string.Format("Number of records with partitionKey 0: {0}", allRecordsByFirstPartition.Count()));

            bool success = true;
            foreach (var selfLink in recordSelfLinks)
            {
                success = success & repo.DeleteAsync(selfLink).Result;
            }
            Console.WriteLine(string.Format("Records deleted successfully: {0}", success));
            Console.ReadLine();
        }
    }

    public class TestDocument : Document
    {
        [JsonProperty("testProp")]
        public string TestProp { get; set; }

        [JsonProperty("partitionKey")]
        public string PartitionKey { get; set; }
    }
}

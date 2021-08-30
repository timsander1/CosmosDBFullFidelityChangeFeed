using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;

namespace FullFidelityChangeFeedDemo
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            ChangeFeedDemo changeFeedDemo = new ChangeFeedDemo();
            await changeFeedDemo.CreateContainerWithFullFidelity();
            await changeFeedDemo.CreateFullFidelityChangeFeedIterator();
            await changeFeedDemo.CreateIncrementalChangeFeedIterator();
            await changeFeedDemo.IngestData();
            await changeFeedDemo.DeleteData();
            await changeFeedDemo.ReadIncrementalChangeFeed();
            await changeFeedDemo.ReadFullFidelityChangeFeed();
        }

    }
}
    



     
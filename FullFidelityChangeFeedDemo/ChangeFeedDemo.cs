using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using System.Collections.Generic;
using Bogus;
using System.Net;
using System.Threading;

namespace FullFidelityChangeFeedDemo
{
    class ChangeFeedDemo
    {
        private static string connectionString;
        private static CosmosClient cosmosClient;
        public Container container;
        private int addItemCounter;
        private int deleteItemCounter;
        public string containerName;
        public string databaseName;
        private string fullFidelityContinuationToken;
        private string readIncrementalContinuationToken;


    public ChangeFeedDemo()
        {
            IConfigurationRoot configuration = new ConfigurationBuilder()
                                   .AddJsonFile("AppSettings.json")
                                   .Build();
            connectionString = configuration["connectionString"];
            containerName = configuration["databaseId"];
            databaseName = configuration["containerId"];
            cosmosClient = new CosmosClient(connectionString, new CosmosClientOptions { ConnectionMode = ConnectionMode.Direct });
            addItemCounter = 0;
            deleteItemCounter = 0;
            fullFidelityContinuationToken = null;
            readIncrementalContinuationToken = null;
        }

    public async Task CreateContainerWithFullFidelity()
        {
            await Console.Out.WriteLineAsync("Creating a container with full fidelity change feed enabled");

            ContainerProperties properties = new ContainerProperties(containerName, partitionKeyPath: "/BuyerState");
            properties.ChangeFeedPolicy.FullFidelityRetention = TimeSpan.FromMinutes(60);

            await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName);
            await cosmosClient.GetDatabase(databaseName).CreateContainerIfNotExistsAsync(properties);

            container = cosmosClient.GetDatabase(databaseName).GetContainer(containerName);
        }

    public async Task CreateFullFidelityChangeFeedIterator()
        {
            fullFidelityContinuationToken = null;
            FeedIterator<ItemWithMetadata> fullfidelityIterator = container
                .GetChangeFeedIterator<ItemWithMetadata>(ChangeFeedStartFrom.Now(), ChangeFeedMode.FullFidelity);
                while (fullfidelityIterator.HasMoreResults)
                {
                    try
                    {
                        FeedResponse<ItemWithMetadata> items = await fullfidelityIterator.ReadNextAsync();
                    }
                    catch (CosmosException cosmosException) when (cosmosException.StatusCode == HttpStatusCode.NotModified)
                    {
                        fullFidelityContinuationToken = cosmosException.Headers.ContinuationToken;
                        Console.WriteLine("Created ChangeFeedIterator to read Full Fidelity change feed");
                        break;
                    }
                }
   
        }

        public async Task CreateIncrementalChangeFeedIterator()
        {
            
            readIncrementalContinuationToken = null;
            FeedIterator<Item> incrementalIterator = container.GetChangeFeedIterator<Item>(ChangeFeedStartFrom.Now(), ChangeFeedMode.Incremental);

                while (incrementalIterator.HasMoreResults)
                {
                    try
                    {
                        FeedResponse<Item> items = await incrementalIterator.ReadNextAsync();
                    }
                    catch (CosmosException cosmosException) when (cosmosException.StatusCode == HttpStatusCode.NotModified)
                    {
                        readIncrementalContinuationToken = cosmosException.Headers.ContinuationToken;
                        Console.WriteLine("Created ChangeFeedIterator to read incremental change feed");
                        break;
                    }
                }
        }

        public async Task IngestData()
        {
            Console.Clear();
           
            await Console.Out.WriteLineAsync("Press any key to begin ingesting data.");

            Console.ReadKey(true);

            await Console.Out.WriteLineAsync("Press any key to stop.");

            var tasks = new List<Task>();

            while (!Console.KeyAvailable)
            {
                foreach (var item in GenerateItems())
                {
                    await container.UpsertItemAsync(item, new PartitionKey(item.BuyerState));
                    Console.Write("*");
                }
            }

            await Task.WhenAll(tasks);
        }

         public async Task DeleteData()
        {
            Console.ReadKey(true);
            Console.Clear();
            await Console.Out.WriteLineAsync("Press any key to begin deleting data.");
            Console.ReadKey(true);

            await Console.Out.WriteLineAsync("Press any key to stop");

            while (!Console.KeyAvailable) {
                deleteItemCounter++;
                try
                {
                    await container.DeleteItemAsync<Item>(
                       partitionKey: new PartitionKey("WA"),
                       id: deleteItemCounter.ToString());
                    Console.Write("-");
                }
                catch
                {

                }
            }
        }

        public async Task ReadFullFidelityChangeFeed()
        {
            Console.ReadKey(true);
            Console.Clear();

            await Console.Out.WriteLineAsync("Press any key to start reading the full fidelity change feed.");

            Console.ReadKey(true);

            FeedIterator<ItemWithMetadata> fullfidelityIterator = container.GetChangeFeedIterator<ItemWithMetadata>(ChangeFeedStartFrom.ContinuationToken(fullFidelityContinuationToken), ChangeFeedMode.FullFidelity, new ChangeFeedRequestOptions { PageSizeHint = 10});

            await Console.Out.WriteLineAsync("Press any key to stop.");

            while (!Console.KeyAvailable)
            {
                while (fullfidelityIterator.HasMoreResults)
                {
                    try
                    {
                        FeedResponse<ItemWithMetadata> items = await fullfidelityIterator.ReadNextAsync();

                        foreach (ItemWithMetadata item in items)
                        {
                            // if operaiton is delete
                            if (item.metadata.operationType == "delete")
                            {
                                if (item.metadata.timeToLiveExpired == true)
                                {
                                    Console.WriteLine($"Operation: {item.metadata.operationType} (due to TTL). Item id: {item.metadata.previousImage.Id}. Previous price: {item.metadata.previousImage.Price}");
                                }
                                else
                                {
                                    Console.WriteLine($"Operation: {item.metadata.operationType} (not due to TTL). Item id: {item.metadata.previousImage.Id}. Previous price: {item.metadata.previousImage.Price}");
                                }
                            }
                            //if operation is replace or insert
                            else
                            {
                                if (item.metadata.previousImage == null)
                                {
                                    Console.WriteLine($"Operation: {item.metadata.operationType}. Item id: {item.Id}. Current price: {item.Price}");
                                }
                                else
                                    Console.WriteLine($"Operation: {item.metadata.operationType}. Item id: {item.Id}. Current price: {item.Price}. Previous price: {item.metadata.previousImage.Price}");

                            }
                        }
                        Thread.Sleep(2000);
                        if (Console.KeyAvailable)
                        {
                            break;
                        }
                    }
                    catch (CosmosException cosmosException)
                    {
                        fullFidelityContinuationToken = cosmosException.Headers.ContinuationToken;
                        Console.WriteLine($"No new changes");
                        Thread.Sleep(3000);
                        if (Console.KeyAvailable)
                        {
                            break;
                        }
                    }
                }
            }
        }

        public async Task ReadIncrementalChangeFeed()
        {
            Console.ReadKey(true);
            Console.Clear();

            await Console.Out.WriteLineAsync("Press any key to begin reading the incremental change feed.");

            Console.ReadKey(true);

            FeedIterator<Item> incrementalIterator = container.GetChangeFeedIterator<Item>(ChangeFeedStartFrom.ContinuationToken(readIncrementalContinuationToken), ChangeFeedMode.Incremental, new ChangeFeedRequestOptions { PageSizeHint = 10 });

            await Console.Out.WriteLineAsync("Press any key to stop.");

            while (!Console.KeyAvailable)
            {
                while (incrementalIterator.HasMoreResults)
                {
                    try
                    {
                        FeedResponse<Item> items = await incrementalIterator.ReadNextAsync();

                        foreach (Item item in items)
                        {
                            // for any operation
                            Console.WriteLine($"Change in item: {item.Id}. New price: {item.Price}.");
                        }
                        Thread.Sleep(2000);
                        if (Console.KeyAvailable)
                        {
                            break;
                        }
                    }
                    catch (CosmosException cosmosException)
                    {
                        fullFidelityContinuationToken = cosmosException.Headers.ContinuationToken;
                        Console.WriteLine($"No new changes");
                        Thread.Sleep(3000);
                        if (Console.KeyAvailable)
                        {
                            break;
                        }
                    }
                }
            }
        }

    private static List<Item> GenerateItems()
        {
            Randomizer random = new Randomizer();

            var states = new string[]
            {
                "WA"
            };

            var prices = new double[]
            {
               3.75, 8.00, 12.00, 10.00,
                17.00, 20.00, 14.00, 15.50,
                9.00, 25.00, 27.00, 21.00, 22.50,
                22.50, 32.00, 30.00, 49.99, 35.50,
                55.00, 50.00, 65.00, 31.99, 79.99,
                22.00, 19.99, 19.99, 80.00, 85.00,
                90.00, 33.00, 25.20, 40.00, 87.50, 99.99,
                95.99, 75.00, 70.00, 65.00, 92.00, 95.00,
                72.00, 25.00, 120.00, 105.00, 130.00, 29.99,
                84.99, 12.00, 37.50
            };

            var items = new List<Item>();

            var stateIndex = random.Number(0, states.Length - 1);
            var pricesIndex = random.Number(0, prices.Length - 1);

            var action = new Item
            {
                Id = random.Number(1, 999).ToString(),
                Price = prices[pricesIndex],
                BuyerState = states[stateIndex]
            };

            items.Add(action);
            return items;
        }
    }
}
    



     
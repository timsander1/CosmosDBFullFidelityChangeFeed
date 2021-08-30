using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace FullFidelityChangeFeedDemo
{

    public class Item
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        public double Price { get; set; }
        public string BuyerState { get; set; }
    }
    public class ItemWithMetadata
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        public double Price { get; set; }
        public string BuyerState { get; set; }

        [JsonProperty("_metadata")]
        public Metadata metadata { get; set; }
    }

    public class Metadata
    {
        [JsonProperty("operationType")]
        public string operationType { get; set; }

        [JsonProperty("timeToLiveExpired")]
        public Boolean timeToLiveExpired { get; set; }

        [JsonProperty("previousImage")]
        public Item previousImage { get; set; }

    }



}

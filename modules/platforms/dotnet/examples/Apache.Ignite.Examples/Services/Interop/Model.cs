using Apache.Ignite.Core.Cache.Configuration;

namespace org.apache.ignite.examples.servicegrid.interop
{
    public class Model
    {
        public int iterationsCount { get; set; }
        public string name { get; set; }
        public CacheMode cacheMode { get; set; }
        public Result[] results { get; set; }
    }
}

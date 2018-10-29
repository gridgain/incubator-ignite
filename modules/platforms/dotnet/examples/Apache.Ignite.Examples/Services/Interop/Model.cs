namespace Org.Apache.Ignite.Examples.Servicegrid.Interop
{
    public class Model
    {
        public int IterationsCount { get; set; }
        public string Name { get; set; }
        public CacheMode CacheMode { get; set; }
        public Result[] Results { get; set; }
    }
}

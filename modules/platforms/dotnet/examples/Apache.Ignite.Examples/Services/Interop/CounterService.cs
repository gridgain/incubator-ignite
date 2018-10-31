using Apache.Ignite.Core.Services;

namespace Apache.Ignite.Examples.Services.Interop
{
    class CounterService : ICounter, IService
    {
        public int increment(int n)
        {
            return n++;
        }

        public void Cancel(IServiceContext context)
        {
        }

        public void Execute(IServiceContext context)
        {
        }

        public void Init(IServiceContext context)
        {
        }
    }
}

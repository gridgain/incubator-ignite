using Apache.Ignite.Core.Binary;

namespace Apache.Ignite.Examples.Services.Interop
{
    public interface ICalculator
    {
        IBinaryObject calculate(IBinaryObject obj);
    }
}

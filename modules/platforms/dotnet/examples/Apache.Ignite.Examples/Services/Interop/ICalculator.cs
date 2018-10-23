using Org.Apache.Ignite.Examples.Servicegrid.Interop;

namespace Apache.Ignite.Examples.Services.Interop
{
    public interface ICalculator
    {
        Model calculate(Model obj);
    }
}

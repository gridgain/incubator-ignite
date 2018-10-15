using Apache.Ignite.Core;
using Apache.Ignite.Core.Communication.Tcp;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;
using System;
using System.IO;

namespace Apache.Ignite.Examples.Services.Interop
{
    class Client
    {
        public static void Main(string[] args)
        {
            string locHost = args.Length > 0 ? args[0] : "127.0.0.1";
            string discoveryAddr = locHost + ":47500";

            var igniteCfg = new IgniteConfiguration
            {
                JvmClasspath = Path.Combine(Environment.GetEnvironmentVariable("IGNITE_HOME"), "examples", "target", "classes"),
                JvmOptions = new[] { "-DIGNITE_QUIET=false", "-Djava.net.preferIPv4Stack=true"/*, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"*/ },

                //ClientMode = true,

                Localhost = locHost,
                DiscoverySpi = new TcpDiscoverySpi
                {
                    LocalAddress = locHost,
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[] { discoveryAddr }
                    }
                },
                CommunicationSpi = new TcpCommunicationSpi
                {
                    LocalAddress = locHost
                },
                MetricsLogFrequency = TimeSpan.Zero
            };

            using (var ignite = Ignition.Start(igniteCfg))
            {
                var svc = ignite.GetServices().GetServiceProxy<IComplexTypeHandler>("ComplexTypeHandler");
                var res = svc.handle(3);

                Console.WriteLine(">>> " + res);
            }
        }
    }
}

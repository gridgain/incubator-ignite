using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
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
            //string locHost = args.Length > 0 ? args[0] : "127.0.0.1";
            //string discoveryAddr = locHost + ":47500";

            var igniteCfg = new IgniteConfiguration
            {
                JvmClasspath = string.Join(
                    ";",
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "core", "target", "libs", "cache-api-1.0.0.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "core", "target", "classes"),

                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "spring", "target", "libs", "commons-logging-1.1.1.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "spring", "target", "libs", "spring-core-4.3.18.RELEASE.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "spring", "target", "libs", "spring-context-4.3.18.RELEASE.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "spring", "target", "libs", "spring-aop-4.3.18.RELEASE.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "spring", "target", "libs", "spring-beans-4.3.18.RELEASE.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "spring", "target", "libs", "spring-expression-4.3.18.RELEASE.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "spring", "target", "classes"),
                    
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "..", "examples", "target", "classes")
                ),
                JvmOptions = new[] { "-DIGNITE_QUIET=false", "-Djava.net.preferIPv4Stack=true", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005" },

                ClientMode = true,

                SpringConfigUrl = Path.Combine("examples", "config", "example-service-interop.xml")

                //Localhost = locHost,
                //BinaryConfiguration = new BinaryConfiguration
                //{
                //    NameMapper = new BinaryBasicNameMapper
                //    {
                //        IsSimpleName = true
                //    },
                //    TypeConfigurations = new[]
                //    {
                //        new BinaryTypeConfiguration
                //        {
                //            TypeName = typeof(ComplexType).Name
                //        }
                //    }
                //},
                //DiscoverySpi = new TcpDiscoverySpi
                //{
                //    LocalAddress = locHost,
                //    IpFinder = new TcpDiscoveryStaticIpFinder
                //    {
                //        Endpoints = new[] { discoveryAddr }
                //    }
                //},
                //CommunicationSpi = new TcpCommunicationSpi
                //{
                //    LocalAddress = locHost
                //},
                //MetricsLogFrequency = TimeSpan.Zero,
                //FailureDetectionTimeout = TimeSpan.FromSeconds(600),
                //ClientFailureDetectionTimeout = TimeSpan.FromSeconds(600)
            };

            using (var ignite = Ignition.Start(igniteCfg))
            {
                var svc = ignite.GetServices().GetServiceProxy<IComplexTypeHandler>("ComplexTypeHandler");
                var res = svc.handle(new ComplexType { i = 3 });

                Console.WriteLine(">>> " + res.i);
            }
        }
    }
}

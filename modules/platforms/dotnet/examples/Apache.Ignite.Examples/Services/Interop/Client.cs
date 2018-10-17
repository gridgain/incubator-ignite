using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache.Configuration;
using org.apache.ignite.examples.servicegrid.interop;
using System;
using System.Collections.Generic;
using System.IO;

namespace Apache.Ignite.Examples.Services.Interop
{
    class Client
    {
        public static void Main(string[] args)
        {
            var igniteCfg = new IgniteConfiguration
            {
                JvmClasspath = string.Join
                (
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
                JvmOptions = new[] { "-Djava.net.preferIPv4Stack=true", "-DIGNITE_QUIET=false" },

                ClientMode = true,

                BinaryConfiguration = new BinaryConfiguration
                {
                    TypeConfigurations = new List<BinaryTypeConfiguration>
                    {
                        new BinaryTypeConfiguration(typeof(Model).FullName),
                        new BinaryTypeConfiguration(typeof(Result).FullName),
                        new BinaryTypeConfiguration(typeof(CacheMode).FullName)
                        {
                            IsEnum = true,
                            NameMapper = new BinaryBasicNameMapper { IsSimpleName = true }
                        }
                    }
                },

                SpringConfigUrl = Path.Combine("examples", "config", "example-service-interop.xml")
            };

            using (var ignite = Ignition.Start(igniteCfg))
            {
                var bin = ignite.GetBinary();
                var svc = ignite.GetServices().WithServerKeepBinary().GetServiceProxy<ICalculator>("Calculator");
                var res = (Model)svc.calculate(bin.ToBinary<IBinaryObject>(new Model { name = "GridGain", iterationsCount = 3, cacheMode = CacheMode.Replicated }));

                Console.WriteLine(">>> " + res.results);
            }
        }
    }
}

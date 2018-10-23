using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache.Configuration;
using Org.Apache.Ignite.Examples.Servicegrid.Interop;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

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
                JvmOptions = new[] 
                {
                    "-Djava.net.preferIPv4Stack=true",
                    "-DIGNITE_QUIET=false",
                    "-DIGNITE_JVM_PAUSE_DETECTOR_DISABLED=true",
                    "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
                },

                WorkDirectory = Path.Combine(Environment.CurrentDirectory, "work"),

                ClientMode = true,

                BinaryConfiguration = new BinaryConfiguration
                {
                    NameMapper = new DotnetToJavaNameMapper(),

                    TypeConfigurations = new List<BinaryTypeConfiguration>
                    {
                        //new BinaryTypeConfiguration(typeof(Model).FullName),
                        //new BinaryTypeConfiguration(typeof(Result).FullName),
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
                var svc = ignite.GetServices().GetServiceProxy<ICalculator>("Calculator");
                var res = svc.calculate(new Model { Name = "GridGain", IterationsCount = 3, CacheMode = CacheMode.Replicated });

                Console.WriteLine(string.Join("\n", res.Results.Select(r => ">>> " + string.Concat(r.Name, " = ", r.Value))));
            }
        }
    }
}

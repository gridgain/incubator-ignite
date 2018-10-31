using Apache.Ignite.Core;
using System;
using System.IO;

namespace Apache.Ignite.Examples.Services.Interop
{
    class Server
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

                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "rest-http", "target", "libs", "commons-lang-2.6.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "rest-http", "target", "libs", "jackson-annotations-2.9.6.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "rest-http", "target", "libs", "jackson-core-2.9.6.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "rest-http", "target", "libs", "jackson-databind-2.9.6.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "rest-http", "target", "libs", "jetty-continuation-9.4.11.v20180605.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "rest-http", "target", "libs", "jetty-http-9.4.11.v20180605.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "rest-http", "target", "libs", "jetty-io-9.4.11.v20180605.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "rest-http", "target", "libs", "jetty-server-9.4.11.v20180605.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "rest-http", "target", "libs", "jetty-util-9.4.11.v20180605.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "rest-http", "target", "libs", "jetty-xml-9.4.11.v20180605.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "rest-http", "target", "libs", "tomcat-servlet-api-9.0.10.jar"),
                    Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", "..", "..", "rest-http", "target", "classes"),

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

                //BinaryConfiguration = new BinaryConfiguration
                //{
                //    NameMapper = new DotnetToJavaNameMapper(),

                //    TypeConfigurations = new List<BinaryTypeConfiguration>
                //    {
                //        new BinaryTypeConfiguration(typeof(Model).FullName),
                //        new BinaryTypeConfiguration(typeof(Result).FullName),
                //        new BinaryTypeConfiguration(typeof(CacheMode).FullName)
                //        {
                //            IsEnum = true,
                //            NameMapper = new BinaryBasicNameMapper { IsSimpleName = true }
                //        }
                //    }
                //}

                //SpringConfigUrl = Path.Combine("examples", "config", "example-service-interop.xml")

                FailureDetectionTimeout = TimeSpan.FromMinutes(10),
                ClientFailureDetectionTimeout = TimeSpan.FromMinutes(10)
            };

            using (var ignite = Ignition.Start(igniteCfg))
            {
                var counter = new CounterService();

                ignite.GetServices().DeployClusterSingleton("Counter", counter);

                Console.WriteLine(">>> Ignite started OK. Press any key to exit...");
                Console.ReadKey();
            }
        }
    }
}

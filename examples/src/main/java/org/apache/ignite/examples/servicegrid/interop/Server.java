package org.apache.ignite.examples.servicegrid.interop;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.nio.file.Paths;
import java.util.Collections;

public class Server {
    public static void main(String[] args) throws IgniteCheckedException {
        String locHost = args.length > 0 ? args[0] : "127.0.0.1";
        String discoveryAddr = locHost + ":47500";

        Ignition.start(
            new IgniteConfiguration()
                .setLocalHost(locHost)
                .setServiceConfiguration(
                    new ServiceConfiguration()
                        .setName("ComplexTypeHandler")
                        .setMaxPerNodeCount(1)
                        .setTotalCount(1)
                        .setService(new ComplexTypeHandlerService())
                )
                .setDiscoverySpi(
                    new TcpDiscoverySpi()
                        .setLocalAddress(locHost)
                        .setIpFinder(
                            new TcpDiscoveryVmIpFinder().setAddresses(Collections.singletonList(discoveryAddr))
                        )
                )
                .setMetricsLogFrequency(0)
                .setGridLogger(
                    new Log4JLogger(Paths.get(System.getenv("IGNITE_HOME"), "config", "ignite-log4j.xml").toString())
                )
        );
    }
}

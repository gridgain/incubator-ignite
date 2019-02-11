package org.apache.ignite.internal.processors.rbcpoc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Collections;

public class Server {
    public static void main(String[] args) {
        Ignition.start(getConfiguration());
    }

    public static IgniteConfiguration getConfiguration() {
        return new IgniteConfiguration()
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(new TcpDiscoveryVmIpFinder()
                    .setAddresses(Collections.singleton("127.0.0.1:47500..47501"))
                )
            )
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(8L * 1024 * 1024 * 1024)
                )
            )
            .setLocalHost("127.0.0.1");
    }
}

package org.apache.ignite.examples.servicegrid.interop;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.platform.dotnet.PlatformDotNetService;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Collections;

public class Client {
    public static void main(String[] args) {
        IgniteConfiguration igniteCfg = new IgniteConfiguration()
            .setClientMode(true)
            .setDiscoverySpi(
                new TcpDiscoverySpi()
                    .setIpFinder(
                        new TcpDiscoveryVmIpFinder().setAddresses(Collections.singletonList("127.0.0.1:47500"))
                    )
            );

        try (Ignite ignite = Ignition.start(igniteCfg)) {
            PlatformDotNetService calc = ignite.services().serviceProxy("Counter", PlatformDotNetService.class, false);

            int in = 0;

            System.out.println(">>> IN: " + in);

            Object out = calc.invokeMethod("increment", false, new Object[] {in});

            System.out.println(">>> OUT: " + out);
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }
    }
}

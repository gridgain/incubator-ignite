/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console;

import java.nio.file.Paths;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * Web Console Launcher.
 */
public class WebConsoleLauncher {
    /**
     * Main entry point.
     *
     * @param args Arguments.
     */
    public static void main(String... args) {
        System.out.println("Starting persistence node for Ignite Web Console Server...");

        String workDir = Paths.get(U.getIgniteHome(), "work-web-console").toString();

        IgniteConfiguration cfg = new IgniteConfiguration()
            .setIgniteInstanceName("Web Console backend")
            .setConsistentId("web-console-backend")
            .setMetricsLogFrequency(0)
            .setLocalHost("127.0.0.1")
            .setWorkDirectory(workDir)
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setLocalPort(60800)
                .setIpFinder(new TcpDiscoveryVmIpFinder()
                    .setAddresses(Collections.singletonList("127.0.0.1:60800"))))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setConnectorConfiguration(null);

        Ignite ignite = Ignition.getOrStart(cfg);

        ignite.cluster().active(true);
    }
}

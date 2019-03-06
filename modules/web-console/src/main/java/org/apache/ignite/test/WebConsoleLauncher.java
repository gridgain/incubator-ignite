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

package org.apache.ignite.test;

import java.io.File;
import java.util.Collections;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.spi.cluster.ignite.IgniteClusterManager;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.WebConsoleServer;
import org.apache.ignite.console.config.WebConsoleConfiguration;
import org.apache.ignite.console.config.WebConsoleConfigurationHelper;
import org.apache.ignite.console.routes.AccountRouter;
import org.apache.ignite.console.routes.AgentDownloadRouter;
import org.apache.ignite.console.routes.ConfigurationsRouter;
import org.apache.ignite.console.routes.NotebooksRouter;
import org.apache.ignite.console.routes.RestApiRouter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * Web Console Launcher.
 */
public class WebConsoleLauncher extends AbstractVerticle {
    /**
     * Main entry point.
     *
     * @param args Arguments.
     */
    public static void main(String... args) {
        System.out.println("Starting Ignite Web Console Server...");

        Ignite ignite = startIgnite();

        VertxOptions options = new VertxOptions()
            .setBlockedThreadCheckInterval(1000L * 60L * 60L)
            .setClusterManager(new IgniteClusterManager(ignite));

        Vertx.clusteredVertx(options, asyncVertx -> {
            if (asyncVertx.succeeded()) {
                Vertx vertx = asyncVertx.result();

                try {
                    ConfigRetriever cfgRetriever = WebConsoleConfigurationHelper.loadConfiguration(
                        vertx,
                        WebConsoleConfigurationHelper.Format.XML,
//                        WebConsoleConfigurationHelper.Format.JSON,
//                        WebConsoleConfigurationHelper.Format.PROPERTIES,
                        "modules/web-console/src/main/resources/web-console.xml",
//                        "modules/web-console/src/main/resources/web-console.json",
//                        "modules/web-console/src/main/resources/web-console.properties",
                        "web-console-config" // Optional, needed only for XML.
                    );

                    cfgRetriever.getConfig(asyncCfg -> {
                        if (asyncCfg.succeeded()) {
                            JsonObject json = asyncCfg.result();

                            WebConsoleConfiguration cfg = json.mapTo(WebConsoleConfiguration.class);

                            RestApiRouter accRouter = new AccountRouter(ignite, vertx);
                            RestApiRouter cfgsRouter = new ConfigurationsRouter(ignite);
                            RestApiRouter notebooksRouter = new NotebooksRouter(ignite);
                            RestApiRouter downloadRouter = new AgentDownloadRouter(ignite, cfg);

                            vertx.deployVerticle(new WebConsoleServer(
                                cfg,
                                ignite,
                                accRouter,
                                cfgsRouter,
                                notebooksRouter,
                                downloadRouter
                            ));

                            System.out.println("Ignite Web Console Server started");
                        }
                        else
                            ignite.log().error("Failed to start Web Console", asyncCfg.cause());

                    });
                }
                catch (Throwable e) {
                    ignite.log().error("Failed to start Web Console", e);
                }
            }
            else
               ignite.log().error("Failed to start Web Console", asyncVertx.cause());
        });
    }

    /**
     * Start Ignite.
     *
     * @return Ignite instance.
     */
    private static Ignite startIgnite() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName("Web Console backend");
        cfg.setConsistentId("web-console-backend");
        cfg.setMetricsLogFrequency(0);
        cfg.setLocalHost("127.0.0.1");

        cfg.setWorkDirectory(new File(U.getIgniteHome(), "work-web-console").getAbsolutePath());

        TcpDiscoverySpi discovery = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:60800"));

        discovery.setLocalPort(60800);
        discovery.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discovery);

        DataStorageConfiguration dataStorageCfg = new DataStorageConfiguration();

        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration();

        dataRegionCfg.setPersistenceEnabled(true);

        dataStorageCfg.setDefaultDataRegionConfiguration(dataRegionCfg);

        cfg.setDataStorageConfiguration(dataStorageCfg);

        cfg.setConnectorConfiguration(null);

        Ignite ignite = Ignition.getOrStart(cfg);

        ignite.cluster().active(true);

        return ignite;
    }
}

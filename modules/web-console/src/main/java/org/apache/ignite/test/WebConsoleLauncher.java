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
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
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
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.repositories.ConfigurationsRepository;
import org.apache.ignite.console.repositories.NotebooksRepository;
import org.apache.ignite.console.services.AccountsService;
import org.apache.ignite.console.services.AdminService;
import org.apache.ignite.console.services.ConfigurationsService;
import org.apache.ignite.console.services.NotebooksService;
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

                DeploymentOptions depOpts = new DeploymentOptions()
                    .setConfig(new JsonObject()
                        .put("configPath", "modules/web-console/src/main/resources/web-console.properties"));

                AccountsRepository accountsRepo = new AccountsRepository(ignite);
                ConfigurationsRepository cfgsRepo = new ConfigurationsRepository(ignite);
                NotebooksRepository notebooksRepo = new NotebooksRepository(ignite);

                vertx.deployVerticle(new AccountsService(ignite, accountsRepo));
                vertx.deployVerticle(new AdminService(ignite, accountsRepo, cfgsRepo, notebooksRepo));
                vertx.deployVerticle(new ConfigurationsService(ignite, cfgsRepo));
                vertx.deployVerticle(new NotebooksService(ignite, notebooksRepo));

                vertx.deployVerticle(new WebConsoleServer(ignite), depOpts);
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

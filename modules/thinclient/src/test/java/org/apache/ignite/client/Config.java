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

package org.apache.ignite.client;

import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

import java.net.*;
import java.util.*;

/** Reusable system tests configuration. */
public class Config {
    /** Host. */
    public static final String HOST = "127.0.0.1";

    /** Name of the cache created by default in the cluster. */
    public static final String DEFAULT_CACHE_NAME = "default";

    /** */
    public static IgniteConfiguration getServerConfiguration() {
        TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.registerAddresses(Collections.singletonList(new InetSocketAddress(HOST, 47500)));

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        discoverySpi.setIpFinder(ipFinder);

        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        igniteCfg.setDiscoverySpi(discoverySpi);

        CacheConfiguration<Integer, Person> dfltCacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        dfltCacheCfg.setIndexedTypes(Integer.class, Person.class);

        igniteCfg.setCacheConfiguration(dfltCacheCfg);

        igniteCfg.setIgniteInstanceName(UUID.randomUUID().toString());

        return igniteCfg;
    }
}

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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.Timer;
import java.util.TimerTask;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.zookeeper.ZooKeeper;

/**
 *
 */
public class ZkPinger extends TimerTask {
    /** Ping interval milliseconds. */
    private static final int DEFAULT_PING_INTERVAL_MS = 2000;

    /** Logger. */
    private final IgniteLogger log;

    /** Zk client. */
    private final ZooKeeper zkClient;

    /** Paths. */
    private final ZkIgnitePaths paths;

    /** Scheduler. */
    private final Timer scheduler = new Timer("ignite-zk-pinger");

    /** Ping interval. */
    private final int pingInterval = DEFAULT_PING_INTERVAL_MS;

    /** Success pings. */
    private int successPings;

    /** Failed pings. */
    private int failedPings;

    /** Attempts. */
    private int attempts;

    /**
     * @param log Logger.
     * @param zkClient Zk client.
     * @param paths Paths.
     */
    public ZkPinger(IgniteLogger log, ZooKeeper zkClient, ZkIgnitePaths paths) {
        this.log = log;
        this.zkClient = zkClient;
        this.paths = paths;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try {
            if (attempts > 0 && attempts % 20 == 0 && log.isInfoEnabled())
                log.info("Zk pinger stats [success=" + successPings + ", failed=" + failedPings + "]");

            attempts++;

            zkClient.exists(paths.clusterDir, false);

            successPings++;
        }
        catch (Throwable t) {
            if (zkClient.getState().isAlive()) {
                U.warn(log, "Failed to ping Zookeeper [zkClient=" + zkClient + "]");

                failedPings++;
            }
            else
                scheduler.cancel();
        }
    }

    /**
     * Starts ping process.
     */
    public void start() {
        scheduler.scheduleAtFixedRate(this, 0, DEFAULT_PING_INTERVAL_MS);

        if (log.isInfoEnabled())
            log.info("Zookeeper pinger has started, ping interval = " + pingInterval + " ms.");
    }

    /**
     * Stops ping process.
     */
    public void stop() {
        try {
            scheduler.cancel();

            if (log.isInfoEnabled())
                log.info("Zookeeper pinger has stopped.");
        }
        catch (Exception e) {
            log.warning("Failed to cancel Zookeeper Pinger scheduler.", e);
        }
    }
}

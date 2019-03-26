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

package org.apache.ignite.console.websocket;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_STATUS;

/**
 * Service that execute various tasks.
 */
@Component
public class ScheduledUpdater {
    /** */
    private static final Logger log = LoggerFactory.getLogger(ScheduledUpdater.class);

    /** */
    private static final String CLUSTER_ID = UUID.randomUUID().toString();

    /** */
    private final Map<String, JsonObject> clusters = new ConcurrentHashMap<>();

    /** */
    private final WebSocketSessions wss;

    /**
     * @param wss Websocket sessions.
     */
    @Autowired
    public ScheduledUpdater(WebSocketSessions wss) {
        this.wss = wss;
    }

    /** */
    @Scheduled(fixedRate = 5000)
    public void updateClusters() {
        try {
            JsonObject desc = clusters.computeIfAbsent(CLUSTER_ID, key -> {
                JsonArray top = new JsonArray();

                top.add(new JsonObject()
                    .put("id", CLUSTER_ID)
                    .put("name", "EMBEDDED")
                    .put("nids", new JsonArray().add(UUID.randomUUID().toString())) // ignite.cluster().localNode().id().toString()))
                    .put("addresses", new JsonArray().add("192.168.0.10"))
                    .put("clusterVersion", "2.8.0") // ignite.version().toString())
                    .put("active", true) // ignite.cluster().active())
                    .put("secured", false)
                );

                return new JsonObject()
                    .put("count", 1)
                    .put("hasDemo", false)
                    .put("clusters", top);
            });

            // TODO IGNITE-5617  Update cluster.active if ignite.cluster().active() state changed!

            wss.broadcastToBrowsers(new WebSocketEvent(
                UUID.randomUUID().toString(),
                AGENT_STATUS,
                desc.toString()
            ));
        }
        catch (Throwable ignore) {
            log.info("Embedded Web Console awaits for cluster activation...");
        }
    }

    /**
     * Periodicakky ping connected clients to keep connections alive.
     */
    @Scheduled(fixedRate = 5000)
    public void pingClients() {
        wss.ping();
    }
}

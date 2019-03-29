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

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.util.JsonUtils.encodeJson;
import static org.apache.ignite.console.util.JsonUtils.fromJson;
import static org.apache.ignite.console.util.JsonUtils.toJson;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketConsts.BROWSERS_PATH;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_STATUS;

/**
 * Websocket sessions service.
 */
@Component
public class WebSocketSessions {
    /** */
    private static final Logger log = LoggerFactory.getLogger(WebSocketSessions.class);

    /** */
    private static final PingMessage PING = new PingMessage(UTF_8.encode("PING"));

    /** */
    private final Map<String, WebSocketSession> agentsSessions;

    /** */
    private final Map<String, WebSocketSession> browsersSessions;

    /** */
    private final Map<String, AgentInfo> agents;

    /** */
    private final Map<String, TopologySnapshot> clusters;

    /**
     * Default constructor.
     */
    public WebSocketSessions() {
        agentsSessions = new ConcurrentHashMap<>();
        browsersSessions = new ConcurrentHashMap<>();
        agents = new ConcurrentHashMap<>();
        clusters = new ConcurrentHashMap<>();
    }

    /**
     * @param ws New websocket sesion.
     */
    public void openSession(WebSocketSession ws) {
        String path = ws.getUri().getPath();

        if (AGENTS_PATH.equals(path))
            agentsSessions.put(ws.getId(), ws);
        else if (BROWSERS_PATH.equals(path))
            browsersSessions.put(ws.getId(), ws);
        else
            log.warn("Unknown path: " + path);
    }

    /**
     * @param ses Sesion to close.
     */
    public void closeSession(WebSocketSession ses) {
        log.info("Removed closed session: " + ses.getId());

        agentsSessions.remove(ses.getId());
        browsersSessions.remove(ses.getId());
    }

    /**
     * @param msg Message to broadcast.
     * @param sockets Sockets.
     */
    private void broadcast(WebSocketMessage msg, Map<String, WebSocketSession> sockets){
        for (Map.Entry<String, WebSocketSession> entry : sockets.entrySet()) {
            WebSocketSession ws = entry.getValue();

            if (ws.isOpen()) {
                try {
                    ws.sendMessage(msg);
                }
                catch (Throwable e) {
                    log.error("Failed to send message to socket: " + ws, e);
                }
            }
            else {
                log.info("Removed closed session: " + ws.getId());

                sockets.remove(entry.getKey());
            }
        }
    }

    /**
     * @param evt Event to broadcast.
     * @param sockets Sockets.
     * @throws IOException If failed.
     */
    private void broadcast(WebSocketEvent evt, Map<String, WebSocketSession> sockets) throws IOException {
        broadcast(new TextMessage(encodeJson(evt)), sockets);
    }

    /**
     * Broadcast event to all connected agents.
     *
     * @param evt Events to send.
     */
    public void broadcastToAgents(WebSocketEvent evt) {
        try {
            broadcast(evt, agentsSessions);
        }
        catch (Throwable e) {
            log.error("Failed to broadcast to agents: " + evt, e);
        }
    }

    /**
     * Broadcast event to all connected browsers.
     *
     * @param evt Events to send.
     */
    public void broadcastToBrowsers(WebSocketEvent evt) {
        try {
            broadcast(evt, browsersSessions);
        }
        catch (Throwable e) {
            log.error("Failed to broadcast event to browsers: " + evt, e);
        }
    }

    /**
     * Ping connected clients.
     */
    public void ping() {
        broadcast(PING, agentsSessions);
        broadcast(PING, browsersSessions);
    }

    /**
     * @param evt Event to process.
     * @throws IOException If failed to process.
     */
    public void registerAgent(WebSocketEvent evt) throws IOException {
        AgentInfo agentInfo = fromJson(evt.getPayload(), AgentInfo.class);

        log.info("Agent connected: " + agentInfo.getAgentId());

        agents.put(agentInfo.getAgentId(), agentInfo);
    }

    /**
     *
     * @param evt Event to process.
     * @throws IOException If failed to update cluster.
     */
    public void registerCluster(WebSocketEvent evt) throws IOException {
        TopologySnapshot top = fromJson(evt.getPayload(), TopologySnapshot.class);

        clusters.put(top.getClusterId(), top);
    }

    /**
     * Notify browsers with current clusters.
     */
    public void updateClusters() {
        try {
            Collection<TopologySnapshot> curClusters = clusters.values();

            Map<String, Object> res = new LinkedHashMap<>();

            res.put("count", curClusters.size());
            res.put("hasDemo", false);
            res.put("clusters", curClusters);

            broadcastToBrowsers(new WebSocketEvent(
                UUID.randomUUID().toString(),
                AGENT_STATUS,
                toJson(res)
            ));
        }
        catch (Throwable e) {
            log.error("Failed to send information about clusters to browsers", e);
        }
    }
}

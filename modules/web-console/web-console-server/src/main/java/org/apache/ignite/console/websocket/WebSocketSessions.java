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
import java.security.Principal;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.console.dto.Account;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.util.JsonUtils.encodeJson;
import static org.apache.ignite.console.util.JsonUtils.fromJson;

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
    private final Map<String, AgentInfo> agents;

    /** */
    private final Map<String, WebSocketSession> agentsSessions;

    /** */
    private final Map<String, WebSocketSession> browsersSessions;

    /** */
    private final Map<String, TopologySnapshot> clusters;

    /**
     * Default constructor.
     */
    public WebSocketSessions() {
        agents = new ConcurrentHashMap<>();
        agentsSessions = new ConcurrentHashMap<>();
        browsersSessions = new ConcurrentHashMap<>();

        clusters = new ConcurrentHashMap<>();
    }

    /**
     * @param ses Session to close.
     */
    public void closeSession(WebSocketSession ses) {
        log.info("Session closed: " + ses);

        // TODO update list of connected agents & cluster

        // agentsSessions.remove(ses.getId());
        // browsersSessions.remove(ses.getId());
    }

    /**
     * @param ws Session.
     * @param msg Message to send.
     * @param tok Token.
     * @param sockets Tokens to sessions map.
     */
    private void sendMessage(
        WebSocketSession ws,
        WebSocketMessage<?> msg,
        String tok,
        Map<String, WebSocketSession> sockets
    ) {
        if (ws.isOpen()) {
            try {
                ws.sendMessage(msg);
            }
            catch (Throwable e) {
                log.error("Failed to send message [token=" + tok + ", session=" + ws + ", msg= " + msg + "]", e);
            }
        }
        else {
            log.info("Removed closed session [token=" + tok + ", session=" + ws + ", msg= " + msg + "]");

            sockets.remove(tok);
        }
    }

    /**
     * @param msg Message to broadcast.
     * @param sessions Tokens to sessions map.
     */
    private void broadcast(WebSocketMessage msg, Map<String, WebSocketSession> sessions) {
        sessions.forEach((tok, ws) -> sendMessage(ws, msg, tok, sessions));
    }

    /**
     * @param evt Event to broadcast.
     * @param sessions Tokens to sessions map..
     * @throws IOException If failed to send event.
     */
    private void broadcast(WebSocketEvent evt, Map<String, WebSocketSession> sessions) throws IOException {
        broadcast(new TextMessage(encodeJson(evt)), sessions);
    }

    /**
     * Broadcast event to all connected agents.
     *
     * @param tok User token.
     * @param evt Event to send.
     */
    public void sendToAgent(String tok, WebSocketEvent evt) {
        try {
            WebSocketSession ws = agentsSessions.get(tok);

            if (ws != null) {
                if (log.isDebugEnabled())
                    log.debug("Found agent session [token=" + tok + ", session=" + ws + "]");

                sendMessage(ws, new TextMessage(encodeJson(evt)), tok, agentsSessions);
            }
            else
                throw new IllegalStateException("Agent not found: " + tok);
        }
        catch (Throwable e) {
            log.error("Failed to send event to agent: " + evt, e);
        }
    }

    /**
     * Broadcast event to all connected browsers.
     *
     * @param evt Events to send.
     */
    public void sendToBrowsers(String tok, WebSocketEvent evt) {
        try {
            log.info("TODO");
            // broadcast(evt, browsersSessions);
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
     * @param ws Session.
     * @throws IOException If failed to process.
     */
    public void registerAgent(WebSocketEvent evt, WebSocketSession ws) throws IOException {
        AgentInfo agentInfo = fromJson(evt.getPayload(), AgentInfo.class);

        log.info("Agent connected: " + agentInfo);

        agents.put(agentInfo.getAgentId(), agentInfo);

        for (String tok : agentInfo.getTokens())
            agentsSessions.put(tok, ws);
    }

    /**
     * Extract user token from session.
     *
     * @param ws Websocket.
     * @return Token.
     */
    private String token(WebSocketSession ws) {
        Principal p = ws.getPrincipal();

        if (p instanceof UsernamePasswordAuthenticationToken) {
            UsernamePasswordAuthenticationToken t = (UsernamePasswordAuthenticationToken)p;

            Object tp = t.getPrincipal();

            if (tp instanceof Account) {
                Account acc = (Account)tp;

                return acc.token();
            }
        }

        throw new IllegalStateException("Token not found");
    }

    /**
     * @param evt Event to process.
     * @param ws Session.
     * @throws IOException If failed to register browser.
     */
    public void registerBrowser(WebSocketEvent evt, WebSocketSession ws) throws IOException {
        BrowserInfo browserInfo = fromJson(evt.getPayload(), BrowserInfo.class);

        log.info("Browser connected: " + browserInfo);

        browsersSessions.put(token(ws), ws);
    }

    /**
     * @param evt Event to process.
     * @throws IOException If failed to register cluster.
     */
    public void registerCluster(WebSocketEvent evt) throws IOException {
        TopologySnapshot top = fromJson(evt.getPayload(), TopologySnapshot.class);

        clusters.put(top.getId(), top);
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

//            broadcastToBrowsers(new WebSocketEvent(
//                UUID.randomUUID().toString(),
//                "backend",
//                AGENT_STATUS,
//                toJson(res)
//            ));
        }
        catch (Throwable e) {
            log.error("Failed to send information about clusters to browsers", e);
        }
    }
}

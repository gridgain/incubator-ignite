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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.console.dto.Account;
import org.jsr166.ConcurrentLinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.util.JsonUtils.errorToJson;
import static org.apache.ignite.console.util.JsonUtils.fromJson;
import static org.apache.ignite.console.util.JsonUtils.toJson;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_INFO;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_STATUS;
import static org.apache.ignite.console.websocket.WebSocketEvents.CLUSTER_TOPOLOGY;
import static org.apache.ignite.console.websocket.WebSocketEvents.ERROR;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_REST;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_VISOR;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_DRIVERS;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_METADATA;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_SCHEMAS;

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
    private final Map<WebSocketSession, AgentInfo> agents;

    /** */
    private final Map<WebSocketSession, TopologySnapshot> clusters;

    /** */
    private final Map<WebSocketSession, String> browsers;

    /** */
    private final Map<String, WebSocketSession> requests;

    /**
     * Default constructor.
     */
    public WebSocketSessions() {
        agents = new ConcurrentLinkedHashMap<>();
        clusters = new ConcurrentHashMap<>();
        browsers = new ConcurrentHashMap<>();
        requests = new ConcurrentHashMap<>();
    }

    /**
     * @param ws Session to close.
     */
    public void closeSession(WebSocketSession ws) {
        log.info("Session closed: " + ws);

        if (AGENTS_PATH.equals(ws.getUri().getPath())) {
            agents.remove(ws);
            clusters.remove(ws);

            updateAgentStatus();
        }
        else
            browsers.remove(ws);
    }

    /**
     * @param wsBrowser Session.
     * @param evt Event.
     * @param errMsg Error message.
     * @param err Error.
     */
    private void sendError(WebSocketSession wsBrowser, WebSocketEvent evt, String errMsg, Throwable err) {
        try {
            evt.setEventType(ERROR);
            evt.setPayload(errorToJson(errMsg, err));

            wsBrowser.sendMessage(new TextMessage(toJson(evt)));
        }
        catch (Throwable e) {
            log.error("Failed to send error message [session=" + wsBrowser + ", event=" + evt + "]", e);
        }
    }

    /**
     * Broadcast event to all connected agents.
     *
     * @param wsBrowser Browser session.
     * @param evt Event to send.
     */
    public void sendToAgent(WebSocketSession wsBrowser, WebSocketEvent evt) {
        try {
            String tok = token(wsBrowser);

            WebSocketSession wsAgent = agents
                .entrySet()
                .stream()
                .filter(e -> e.getValue().getTokens().contains(tok))
                .findFirst()
                .map(Map.Entry::getKey)
                .orElseThrow(() -> new IllegalStateException("Agent not found for token: " + tok));

            if (log.isDebugEnabled())
                log.debug("Found agent session [token=" + tok + ", session=" + wsAgent + ", event=" + evt + "]");

            wsAgent.sendMessage(new TextMessage(toJson(evt)));
        }
        catch (Throwable e) {
            String errMsg = "Failed to send event to agent: " + evt;

            log.error(errMsg, e);

            sendError(wsBrowser, evt, errMsg, e);
        }
    }

    /**
     * @param ws Session to ping.
     */
    private void ping(WebSocketSession ws) {
        try {
            if (ws.isOpen())
                ws.sendMessage(PING);
        }
        catch (Throwable e) {
            log.error("Failed to send PING request [session=" + ws + "]");
        }
    }

    /**
     * Ping connected clients.
     */
    public void ping() {
        agents.keySet().forEach(this::ping);
        browsers.keySet().forEach(this::ping);
    }

    /**
     * @param evt Event to process.
     * @param ws Session.
     * @throws IOException If failed to process.
     */
    public void registerAgent(WebSocketEvent evt, WebSocketSession ws) throws IOException {
        AgentInfo agentInfo = fromJson(evt.getPayload(), AgentInfo.class);

        log.info("Agent connected: " + agentInfo);

        agents.put(ws, agentInfo);
    }

    /**
     * @param ws Session.
     */
    public void registerBrowser(WebSocketSession ws) {
        log.info("Browser connected: " + ws);

        browsers.put(ws, token(ws));

        updateAgentStatus();
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

        log.error("Token not found [session=" + ws + "]");

        return UUID.randomUUID().toString();
    }

    /**
     * @param ws Websocket.
     * @param msg Incoming message.
     * @throws IOException If failed to handle event.
     */
    public void handleAgentEvents(WebSocketSession ws, TextMessage msg) throws IOException {
        WebSocketEvent evt = fromJson(msg.getPayload(), WebSocketEvent.class);

        switch (evt.getEventType()) {
            case AGENT_INFO:
                registerAgent(evt, ws);

                break;

            case CLUSTER_TOPOLOGY:
                registerCluster(ws, evt);

                break;

            default:
                WebSocketSession browserWs = requests.remove(evt.getRequestId());

                if (browserWs != null)
                    browserWs.sendMessage(new TextMessage(toJson(evt)));
                else
                    log.warn("Failed to send event to browser: " + evt);
        }
    }

    /**
     * @param ws Websocket.
     * @param msg Incoming message.
     * @throws IOException If failed to handle event.
     */
    public void handleBrowserEvents(WebSocketSession ws, TextMessage msg) throws IOException {
        WebSocketEvent evt = fromJson(msg.getPayload(), WebSocketEvent.class);

        switch (evt.getEventType()) {
            case SCHEMA_IMPORT_DRIVERS:
            case SCHEMA_IMPORT_SCHEMAS:
            case SCHEMA_IMPORT_METADATA:
            case NODE_REST:
            case NODE_VISOR:
                requests.put(evt.getRequestId(), ws);

                sendToAgent(ws, evt);

                break;

            default:
                log.warn("Unknown event: " + evt);
        }
    }

    /**
     * @param ws Session.
     * @param evt Event to process.
     */
    public void registerCluster(WebSocketSession ws, WebSocketEvent evt) {
        try {
            TopologySnapshot top = fromJson(evt.getPayload(), TopologySnapshot.class);

            clusters.put(ws, top);

            updateAgentStatus();
        }
        catch (Throwable e) {
            log.error("Failed to send information about clusters to browsers", e);
        }
    }

    /**
     * Send to all connected browsers info about agent status.
     */
    private void updateAgentStatus() {
        browsers.forEach((wsBrowser, tok) -> {
            List<TopologySnapshot> tops = new ArrayList<>();

            agents.forEach((wsAgent, agentInfo) -> {
               if (agentInfo.getTokens().contains(tok)) {
                   TopologySnapshot top = clusters.get(wsAgent);

                   if (top != null && tops.stream().allMatch(t -> t.differentCluster(top)))
                       tops.add(top);
               }
            });

            Map<String, Object> res = new LinkedHashMap<>();

            res.put("count", tops.size());
            res.put("hasDemo", false);
            res.put("clusters", tops);

            try {
                wsBrowser.sendMessage(new TextMessage(
                    toJson(new WebSocketEvent(
                        UUID.randomUUID().toString(),
                        AGENT_STATUS,
                        toJson(res)))
                ));
            }
            catch (Throwable e) {
                log.error("Failed to update agent status [session=" + wsBrowser + ", token=" + tok + "]", e);
            }
        });
    }
}

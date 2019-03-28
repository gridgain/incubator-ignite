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
import java.util.Map;
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
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketConsts.BROWSERS_PATH;

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
    private final Map<String, WebSocketSession> agents = new ConcurrentHashMap<>();

    /** */
    private final Map<String, WebSocketSession> browsers = new ConcurrentHashMap<>();

    /**
     * @param ws New websocket sesion.
     */
    public void openSession(WebSocketSession ws) {
        String path = ws.getUri().getPath();

        if (AGENTS_PATH.equals(path))
            agents.put(ws.getId(), ws);
        else if (BROWSERS_PATH.equals(path))
            browsers.put(ws.getId(), ws);
        else
            log.warn("Unknown path: " + path);
    }

    /**
     * @param ses Sesion to close.
     */
    public void closeSession(WebSocketSession ses) {
        log.info("Removed closed session: " + ses.getId());

        agents.remove(ses.getId());
        browsers.remove(ses.getId());
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
            broadcast(evt, agents);
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
            broadcast(evt, browsers);
        }
        catch (Throwable e) {
            log.error("Failed to broadcast event to browsers: " + evt, e);
        }
    }

    /**
     * Ping connected clients.
     */
    public void ping() {
        broadcast(PING, agents);
        broadcast(PING, browsers); // TODO IGNITE-5617 Not sure if we need to ping browser, investigate later.
    }
}

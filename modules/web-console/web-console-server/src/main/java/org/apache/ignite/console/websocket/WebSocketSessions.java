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
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import static org.apache.ignite.console.Utils.encodeJson;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketConsts.BROWSERS_PATH;

/**
 *
 */
@Component
public class WebSocketSessions {
    /** */
    private static final Logger log = LoggerFactory.getLogger(WebSocketSessions.class);

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
     * @param evt Event to broadcast.
     * @param sockets Sockets.
     * @throws Exception If failed to broadcast.
     */
    private void broadcast(WebSocketEvent evt, Map<String, WebSocketSession> sockets) throws Exception {
        for (Map.Entry<String, WebSocketSession> entry : sockets.entrySet()) {
            WebSocketSession ws = entry.getValue();

            if (ws.isOpen())
                ws.sendMessage(new TextMessage(encodeJson(evt)));
            else {
                log.info("Removed closed session: " + ws.getId());

                browsers.remove(entry.getKey());
            }
        }
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
}

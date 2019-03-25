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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;


import static org.apache.ignite.console.Utils.encodeJson;

/**
 *
 */
@Component
public class WebSocketSessions {
    /** */
    private static final Logger log = LoggerFactory.getLogger(WebSocketSessions.class);

    /** */
    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    /**
     * @return Collection of current sessions.
     */
    public Collection<WebSocketSession> sessions() {
        return sessions.values();
    }

    /**
     * @param ses New sesion.
     */
    public void openSession(WebSocketSession ses) {
        sessions.put(ses.getId(), ses);
    }

    /**
     * @param ses Sesion to close.
     */
    public void closeSession(WebSocketSession ses) {
        log.info("Removed closed session: " + ses.getId());

        sessions.remove(ses.getId());
    }

    /**
     * @param evt Event to send.
     */
    public void sendEvent(WebSocketEvent evt) {
        try {
            for (Map.Entry<String, WebSocketSession> entry : sessions.entrySet()) {
                WebSocketSession ws = entry.getValue();

                if (ws.isOpen())
                    ws.sendMessage(new TextMessage(encodeJson(evt)));
                else {
                    log.info("Removed closed session: " + ws.getId());

                    sessions.remove(entry.getKey());
                }
            }
        }
        catch (Throwable e) {
            log.info("Error: " + e.getMessage());
        }
    }
}

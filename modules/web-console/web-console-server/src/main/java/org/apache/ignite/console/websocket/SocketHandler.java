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

import org.apache.ignite.console.rest.RestApiController;
import org.apache.ignite.console.websocket.AgentInfo;
import org.apache.ignite.console.websocket.BrowserInfo;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.console.websocket.WebSocketSessions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import static org.apache.ignite.console.Utils.toAgentInfo;
import static org.apache.ignite.console.Utils.toBrowserInfo;
import static org.apache.ignite.console.Utils.toWsEvt;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_INFO;
import static org.apache.ignite.console.websocket.WebSocketEvents.BROWSER_INFO;

/**
 * Todo
 */
@Component
public class SocketHandler extends TextWebSocketHandler {
    /** */
    private static final Logger log = LoggerFactory.getLogger(RestApiController.class);

    /** */
    private final WebSocketSessions wss;

    /**
     * @param wss Websockets sessions.
     */
    public SocketHandler(WebSocketSessions wss) {
        this.wss = wss;
    }

    /** {@inheritDoc} */
    @Override public void handleTextMessage(WebSocketSession ses, TextMessage msg) {
            try {
                for (WebSocketSession ws : wss.sessions()) {
                    if (ws.isOpen()) {
                        String payload = msg.getPayload();

                        log.info("WS Request:  [ses: " + ws.getId() + ", data: " + payload + "]");

                        WebSocketEvent evt = toWsEvt(payload);

                        String evtType = evt.getEventType();

                        switch (evtType) {
                            case AGENT_INFO:
                                AgentInfo agentInfo = toAgentInfo(evt.getPayload());

                                log.info("Web Console agent connected: " + agentInfo);

                                break;

                            case BROWSER_INFO:
                                BrowserInfo browserInfo = toBrowserInfo(evt.getPayload());

                                log.info("Browser connected: " + browserInfo);

                                break;

                            default:
                                log.info("Unknown event: " + evt);
                        }
                    }
                    else
                        wss.closeSession(ws);
                }
            }
            catch (Throwable e) {
                log.error("Failed to process incoming message", e);
            }
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionEstablished(WebSocketSession ws) {
        //Messages will be sent to all users.
        wss.openSession(ws);
    }
}

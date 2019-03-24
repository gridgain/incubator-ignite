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

package org.apache.ignite.console;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.console.websocket.AgentInfo;
import org.apache.ignite.console.websocket.BrowserInfo;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_INFO;
import static org.apache.ignite.console.websocket.WebSocketEvents.BROWSER_INFO;

/**
 * Todo
 */
@Component
public class SocketHandler extends TextWebSocketHandler {
    /** */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** */
    List<WebSocketSession> sessions = new CopyOnWriteArrayList<>();

    /** {@inheritDoc} */
    @Override public void handleTextMessage(WebSocketSession ses, TextMessage msg) {
            try {
                for (WebSocketSession ws : sessions) {
                    if (ws.isOpen()) {
                        String payload = msg.getPayload();

                        System.out.println("WS Request:  [ses: " + ws.getId() + ", data: " + payload + "]");

                        WebSocketEvent evt = MAPPER.readValue(payload, WebSocketEvent.class);

                        String evtType = evt.getEventType();

                        switch (evtType) {
                            case AGENT_INFO:
                                AgentInfo agentInfo = MAPPER.readValue(evt.getData(), AgentInfo.class);

                                System.out.println("Web Console agent connected: " + agentInfo);

                                break;

                            case BROWSER_INFO:
                                BrowserInfo browserInfo = MAPPER.readValue(evt.getData(), BrowserInfo.class);

                                System.out.println("Browser connected: " + browserInfo);

                                break;

                            default:
                                System.out.println("Unknown event: " + evt);
                        }
                    }
                    else {
                        System.out.println("Removed closed session: " + ws.getId());

                        sessions.remove(ws);
                    }
                }
            }
            catch (Throwable e) {
                System.out.println("Error: " + e.getMessage());
            }
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionEstablished(WebSocketSession ses) {
        System.out.println("New session: " + ses.getId());

        //Messages will be sent to all users.
        sessions.add(ses);
    }
}

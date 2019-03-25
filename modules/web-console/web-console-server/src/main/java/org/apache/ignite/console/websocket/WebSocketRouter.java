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
import org.apache.ignite.console.rest.RestApiController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import static org.apache.ignite.console.Utils.encodeJson;
import static org.apache.ignite.console.Utils.toAgentInfo;
import static org.apache.ignite.console.Utils.toBrowserInfo;
import static org.apache.ignite.console.Utils.toWsEvt;
import static org.apache.ignite.console.websocket.WebSocketConsts.BROWSERS_PATH;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_INFO;
import static org.apache.ignite.console.websocket.WebSocketEvents.BROWSER_INFO;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_REST;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_VISOR;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_DRIVERS;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_METADATA;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_SCHEMAS;

/**
 * Router for requests from web sockets.
 */
@Component
public class WebSocketRouter extends TextWebSocketHandler {
    /** */
    private static final Logger log = LoggerFactory.getLogger(RestApiController.class);

    /** */
    private final WebSocketSessions wss;

    /** */
    private final Map<String, WebSocketSession> requests;

    /**
     * @param wss Websocket sessions.
     */
    public WebSocketRouter(WebSocketSessions wss) {
        this.wss = wss;

        requests = new ConcurrentHashMap<>();
    }

    /** {@inheritDoc} */
    @Override public void handleTextMessage(WebSocketSession ws, TextMessage msg) {
            try {
                boolean fromBrowser = BROWSERS_PATH.equals(ws.getUri().getPath());

                String payload = msg.getPayload();

                WebSocketEvent evt = toWsEvt(payload);

                String evtType = evt.getEventType();

                switch (evtType) {
                    case AGENT_INFO:
                        AgentInfo agentInfo = toAgentInfo(evt.getPayload());

                        log.info("Agent connected: " + agentInfo.getAgentId());

                        break;

                    case BROWSER_INFO:
                        BrowserInfo browserInfo = toBrowserInfo(evt.getPayload());

                        log.info("Browser connected: " + browserInfo.getBrowserId());

                        break;

                    case SCHEMA_IMPORT_DRIVERS:
                    case SCHEMA_IMPORT_SCHEMAS:
                    case SCHEMA_IMPORT_METADATA:
                    case NODE_REST:
                    case NODE_VISOR:
                        if (fromBrowser) {
                            log.info("evtType: " + evtType + ", reqId: " + evt.getRequestId() + ", ws: " + ws.getId());

                            requests.put(evt.getRequestId(), ws);

                            // TODO IGNITE-5617: select correct agent.
                            wss.broadcastToAgents(evt);
                        }
                        else {
                            WebSocketSession browserWs = requests.remove(evt.getRequestId());

                            log.info("evtType: " + evtType + ", reqId: " + evt.getRequestId() +
                                ", ws: " + ws.getId() + ", browserWs: " + browserWs.getId());

                            if (browserWs != null)
                                browserWs.sendMessage(new TextMessage(encodeJson(evt)));
                            else
                                log.warn("Failed to send response: " + evt.getRequestId());
                        }

                        break;

                    default:
                        log.info("Unknown event: " + evt);
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

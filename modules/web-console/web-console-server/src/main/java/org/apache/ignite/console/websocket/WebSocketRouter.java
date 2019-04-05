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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PongMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import static org.apache.ignite.console.websocket.WebSocketConsts.BROWSERS_PATH;

/**
 * Router for requests from web sockets.
 */
@Component
public class WebSocketRouter extends TextWebSocketHandler {
    /** */
    private static final Logger log = LoggerFactory.getLogger(WebSocketRouter.class);

    /** */
    private final WebSocketSessions wss;

    /**
     * @param wss Websocket sessions.
     */
    public WebSocketRouter(WebSocketSessions wss) {
        this.wss = wss;
    }

    /** {@inheritDoc} */
    @Override public void handleTextMessage(WebSocketSession ws, TextMessage msg) {
        try {
            if (BROWSERS_PATH.equals(ws.getUri().getPath()))
                wss.handleBrowserEvents(ws, msg);
            else
                wss.handleAgentEvents(ws, msg);
        }
        catch (Throwable e) {
            log.error("Failed to process websocket message [session=" + ws + ", msg=" + msg + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void handlePongMessage(WebSocketSession ws, PongMessage msg) {
        try {
            if (log.isTraceEnabled())
                log.trace("Received pong message [socket=" + ws + ", msg=" + msg + "]");
        }
        catch (Throwable e) {
            log.error("Failed to process pong message [socket=" + ws + ", msg=" + msg + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionEstablished(WebSocketSession ws) {
        log.info("Session opened [socket=" + ws + "]");

        ws.setTextMessageSizeLimit(10 * 1024 * 1024); // TODO IGNITE-5617 how to configure in more correct way

        if (BROWSERS_PATH.equals(ws.getUri().getPath()))
            wss.registerBrowser(ws);
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionClosed(WebSocketSession ws, CloseStatus status) {
        log.info("Session closed [socket=" + ws + ", status=" + status + "]");

        wss.closeSession(ws);
    }
}

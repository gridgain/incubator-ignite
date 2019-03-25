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

package org.apache.ignite.console.agent.handlers;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.LoggerFactory;

import static org.apache.ignite.console.agent.AgentUtils.toJson;

/**
 *
 */
public class WebSocketSession {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(WebSocketSession.class));

    /** */
    private final AtomicReference<Session> sesRef;

    /**
     * Default constructor.
     */
    public WebSocketSession() {
        sesRef = new AtomicReference<>();
    }

    /**
     * @param ses New session.
     */
    public void open(Session ses) {
        sesRef.set(ses);
    }

    /**
     * Close current sesion.
     */
    public void close() {
        sesRef.set(null);
    }

    /**
     * Send event to websocket.
     *
     * @param evt Event.
     */
    public void send(WebSocketEvent evt) {
        try {
            Session ses = sesRef.get();

            if (ses == null)
                throw new IOException("No active session");

            ses.getRemote().sendString(toJson(evt));
        }
        catch (Throwable e) {
            log.error("Failed to send event", e);
        }
    }

    /**
     * Send event to websocket.
     *
     * @param evtType Event type.
     * @param val Object to send as payload.
     */
    public void send(String evtType, Object val) {
        try {
            send(new WebSocketEvent(
                UUID.randomUUID().toString(),
                evtType,
                toJson(val)
            ));
        }
        catch (Throwable e) {
            log.error("Failed to send event", e);
        }
    }
}

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

import static org.apache.ignite.console.util.JsonUtils.errorToJson;
import static org.apache.ignite.console.util.JsonUtils.toJson;
import static org.apache.ignite.console.websocket.WebSocketEvents.ERROR;

/**
 * Wrapper for websocket.
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
     * @throws IOException If failed to send event.
     */
    public void send(WebSocketEvent evt) throws IOException {
        Session ses = sesRef.get();

        if (ses == null)
            throw new IOException("No active session");

        ses.getRemote().sendString(toJson(evt));
    }

    /**
     * Send event to websocket.
     *
     * @param evtType Event type.
     * @param payload Payload.
     * @throws IOException If failed to send event.
     */
    public void send(String evtType, Object payload) throws IOException {
        send(new WebSocketEvent(
            UUID.randomUUID().toString(),
            evtType,
            toJson(payload)
        ));
    }

    /**
     * Reply with result.
     *
     * @param evt Source event.
     * @param res Result.
     * @throws IOException If failed.
     */
    public void reply(WebSocketEvent evt, Object res) throws IOException {
        evt.setPayload(toJson(res));

        send(evt);
    }

    /**
     * Reply with error message.
     *
     * @param evt Source event.
     * @param errMsg Error message.
     * @param cause Error cause.
     */
    public void fail(WebSocketEvent evt, String errMsg, Throwable cause) {
        try {
            evt.setEventType(ERROR);
            evt.setPayload(errorToJson(errMsg, cause));

            send(evt);
        }
        catch (Throwable e) {
            log.error("Failed to send error message: [msg=" + errMsg + ", cause=" + cause + "]", e);
        }
    }
}

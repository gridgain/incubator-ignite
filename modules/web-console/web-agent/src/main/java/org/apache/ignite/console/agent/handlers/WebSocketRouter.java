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

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.websocket.AgentInfo;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketFrame;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.api.extensions.Frame;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.agent.AgentUtils.sslContextFactory;
import static org.apache.ignite.console.util.JsonUtils.fromJson;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_INFO;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_REST;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_VISOR;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_DRIVERS;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_METADATA;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_SCHEMAS;

/**
 * Router that listen for web socket and redirect messages to event bus.
 */
@WebSocket(maxTextMessageSize = 10 * 1024 * 1024, maxBinaryMessageSize = 10 * 1024 * 1024)
public class WebSocketRouter implements AutoCloseable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(WebSocketRouter.class));

    /** */
    private static final String AGENT_ID = UUID.randomUUID().toString();

    /** */
    private static final ByteBuffer PONG_MSG = UTF_8.encode("PONG");

    /** */
    private final AgentConfiguration cfg;

    /** */
    private final CountDownLatch closeLatch;

    /** */
    private final WebSocketSession wss;

    /** */
    private final ClusterHandler clusterHnd;

    /** */
    private final DatabaseHandler dbHnd;

    /** */
    private WebSocketClient client;


    /**
     * @param cfg Configuration.
     * @throws Exception If failed to create websocket handler.
     */
    public WebSocketRouter(AgentConfiguration cfg) throws Exception {
        this.cfg = cfg;

        closeLatch = new CountDownLatch(1);
        wss = new WebSocketSession(AGENT_ID);
        clusterHnd = new ClusterHandler(cfg, wss);
        dbHnd = new DatabaseHandler(cfg, wss);
    }

    /**
     * Start websocket client.
     */
    public void start() {
        log.info("Web Agent ID: " + AGENT_ID);
        log.info("Connecting to: " + cfg.serverUri());

        connect();

        clusterHnd.start();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            client.stop();
        }
        catch (Throwable e) {
            log.error("Failed to close websocket", e);
        }

        try {
            clusterHnd.stop();
        }
        catch (Throwable e) {
            log.error("Failed to stop cluster handler", e);
        }
    }

    /**
     * Connect to websocket.
     */
    private void connect() {
        boolean trustAll = Boolean.getBoolean("trust.all");

        if (trustAll && !F.isEmpty(cfg.serverTrustStore())) {
            log.warning("Options contains both '--server-trust-store' and '-Dtrust.all=true'. " +
                "Option '-Dtrust.all=true' will be ignored on connect to Web server.");

            trustAll = false;
        }

        boolean ssl = trustAll || !F.isEmpty(cfg.serverTrustStore()) || !F.isEmpty(cfg.serverKeyStore());

        if (ssl) {
            SslContextFactory sslCtxFactory = sslContextFactory(
                cfg.serverKeyStore(),
                cfg.serverKeyStorePassword(),
                trustAll,
                cfg.serverTrustStore(),
                cfg.serverTrustStorePassword(),
                cfg.cipherSuites()
            );

            client = new WebSocketClient(sslCtxFactory);
        }
        else
            client = new WebSocketClient();

        reconnect();
    }

    /**
     * Reconnect to backend.
     */
    private void reconnect() {
        try {
            client.start();
            client.connect(this, new URI(cfg.serverUri() + "/agents"));
        }
        catch (Exception e) {
            log.error("Unable to connect to WebSocket: ", e);
        }
    }

    /**
     * @throws InterruptedException If await failed.
     */
    public void awaitClose() throws InterruptedException {
        closeLatch.await();
    }

    /**
     *
     * @param statusCode Close status code.
     * @param reason Close reason.
     */
    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        log.info("Connection closed [code=" + statusCode + ", reason=" + reason + "]");

        wss.close();

        connect();
    }

    /**
     * @param ses Session.
     */
    @OnWebSocketConnect
    public void onConnect(Session ses) {
        log.info("Connected to server: " + ses.getRemoteAddress());

        wss.open(ses);

        try {
            wss.send(AGENT_INFO, new AgentInfo(AGENT_ID, cfg.tokens()));
        }
        catch (Throwable e) {
            log.error("Failed to send agent info to server", e);
        }
    }

    /**
     * @param msg Message.
     */
    @OnWebSocketMessage
    public void onMessage(String msg) {
        try {
            WebSocketEvent evt = fromJson(msg, WebSocketEvent.class);

            String evtType = evt.getEventType();

            switch (evtType) {
                case SCHEMA_IMPORT_DRIVERS:
                    dbHnd.collectJdbcDrivers(evt);

                    break;

                case SCHEMA_IMPORT_SCHEMAS:
                    dbHnd.collectDbSchemas(evt);
                    break;

                case SCHEMA_IMPORT_METADATA:
                    dbHnd.collectDbMetadata(evt);
                    break;

                case NODE_REST:
                case NODE_VISOR:
                    clusterHnd.restRequest(evt);
                    break;

                default:
                    log.warning("Unknown event: " + evt);
            }
        }
        catch (Throwable e) {
            log.error("Failed to process message: " + msg, e);
        }
    }

    /**
     * @param ses Session.
     * @param frame Frame.
     */
    @OnWebSocketFrame
    public void onFrame(Session ses, Frame frame) {
        if (frame.getType() == Frame.Type.PING) {
            if (log.isTraceEnabled())
                log.trace("Received ping message [socket=" + ses + ", msg=" + frame + "]");

            try {
                ses.getRemote().sendPong(PONG_MSG);
            }
            catch (Throwable e) {
                log.error("Failed to send pong to: " + ses, e);
            }
        }
    }

    /**
     * @param e Error.
     */
    @OnWebSocketError
    public void onError(Throwable e) {
        log.error("ERR on websocket", e);

        try {
            Thread.sleep(3000);
        }
        catch (Throwable ignore) {
        }

        reconnect();
    }
}

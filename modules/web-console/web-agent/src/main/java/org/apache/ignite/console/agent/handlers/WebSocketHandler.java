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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.websocket.AgentInfo;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.LoggerFactory;

import static org.apache.ignite.console.agent.AgentUtils.fromJson;
import static org.apache.ignite.console.agent.AgentUtils.sslContextFactory;
import static org.apache.ignite.console.agent.AgentUtils.toJson;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_INFO;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_DRIVERS;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_METADATA;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_SCHEMAS;

/**
 * Router that listen for web socket and redirect messages to event bus.
 */
@WebSocket
public class WebSocketHandler implements AutoCloseable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(WebSocketHandler.class));

    /** */
    private static final String AGENT_ID = UUID.randomUUID().toString();

    /** */
    private final AgentConfiguration cfg;

    /** */
    private final CountDownLatch closeLatch;

    /** */
    private final WebSocketSession webSocketSes;

    /** */
    private final ClusterHandler clusterHandler;

    /** */
    private final DatabaseHandler dbHandler;

    /** */
    private WebSocketClient client;


    /**
     * @param cfg Configuration.
     * @throws Exception If failed to create websocket handler.
     */
    public WebSocketHandler(AgentConfiguration cfg) throws Exception {
        this.cfg = cfg;

        closeLatch = new CountDownLatch(1);
        webSocketSes = new WebSocketSession();
        clusterHandler = new ClusterHandler(cfg);
        dbHandler = new DatabaseHandler(cfg);
    }

    /**
     *
     */
    public void start() {
        // RestExecutor restExecutor = new RestExecutor(cfg);

        log.info("Web Agent ID: " + AGENT_ID);
        log.info("Connecting to: " + cfg.serverUri());

        connect();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            client.stop();
        }
        catch (Throwable e) {
            log.error("Failed to close websocket", e);
        }
    }

    /**
     * Connect to websocket.
     */
    private void connect() {
        boolean serverTrustAll = Boolean.getBoolean("trust.all");

        if (serverTrustAll && !F.isEmpty(cfg.serverTrustStore())) {
            log.warning("Options contains both '--server-trust-store' and '-Dtrust.all=true'. " +
                "Option '-Dtrust.all=true' will be ignored on connect to Web server.");

            serverTrustAll = false;
        }

        boolean ssl = serverTrustAll || !F.isEmpty(cfg.serverTrustStore()) || !F.isEmpty(cfg.serverKeyStore());

        if (ssl) {
            SslContextFactory sslCtxFactory = sslContextFactory(
                cfg.serverKeyStore(),
                cfg.serverKeyStorePassword(),
                serverTrustAll,
                cfg.serverTrustStore(),
                cfg.serverTrustStorePassword(),
                cfg.cipherSuites()
            );

            HttpClient httpClient = new HttpClient(sslCtxFactory);

            client = new WebSocketClient(httpClient);
        }
        else
            client = new WebSocketClient();

        reconnect();
    }

    /**
     *
     */
    private void reconnect() {
        try {
            client.start();
            ClientUpgradeRequest request = new ClientUpgradeRequest();
            client.connect(this, new URI(cfg.serverUri() + "/agents"), request);
        } catch (Exception e) {
            log.error("Unable to connect to WebSocket: ", e);
        }
    }

//
//    /**
//     * Connect to server.
//     *
//     * @param tid Timer ID.
//     */
//    @SuppressWarnings("unused")
//    private void connect(long tid) {
//        client.websocket(conOpts,
//            ws -> {
//                log.info("Connected to server: " + ws.remoteAddress());
//
//                register(ws, Addresses.SCHEMA_IMPORT_DRIVERS);
//                register(ws, Addresses.SCHEMA_IMPORT_SCHEMAS);
//                register(ws, Addresses.SCHEMA_IMPORT_METADATA);
//                register(ws, Addresses.NODE_REST);
//                register(ws, Addresses.NODE_VISOR);
//
//                MessageConsumer c1 = vertx.eventBus().consumer(Addresses.CLUSTER_TOPOLOGY, msg -> {
//                    JsonObject json = new JsonObject()
//                        .put("agentId", AGENT_ID)
//                        .put("top", msg.body());
//
//                    send(ws, Addresses.CLUSTER_TOPOLOGY, json);
//                });
//
//                ws.handler(data -> {
//                    JsonObject json = data.toJsonObject();
//
//                    String addr = json.getString("address");
//
//                    if (F.isEmpty(addr))
//                        log.error("Unexpected request: " + json);
//                    else
//                        vertx.eventBus().send(
//                            addr,
//                            json.getJsonObject("body"),
//                            asyncRes -> {
//                                Object body = asyncRes.succeeded()
//                                    ? asyncRes.result().body()
//                                    : new JsonObject().put("err", asyncRes.cause().getMessage());
//
//                                send(ws, json.getString("replyAddress"), body);
//                            });
//
//                    // log.info("Received data " + data.toString());
//                });
//
//                long pingTimer = vertx.setPeriodic(3000, v -> write(ws, PING));
//
//                ws.closeHandler(p -> {
//                    log.warning("Connection closed: " + ws.remoteAddress());
//
//                    vertx.cancelTimer(pingTimer);
//                    c1.unregister();
//
//                    curTimer = vertx.setTimer(1, this::connect);
//                });
//
//                ws.exceptionHandler(e -> log.error("EXCEPTION ON SOCKET", e));
//                // send(ws, "agent:id", AGENT_ID);
//            },
//            e -> {
//                LT.warn(log, e.getMessage());
//
//                curTimer = vertx.setTimer(3000, this::connect);
//            });
//    }

    /**
     *
     * @throws InterruptedException
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

        webSocketSes.close();
    }

    /**
     * @param ses Session.
     */
    @OnWebSocketConnect
    public void onConnect(Session ses) {
        log.info("Connected to server: " + ses.getRemoteAddress());

        webSocketSes.open(ses);

        webSocketSes.send(AGENT_INFO, new AgentInfo(AGENT_ID, "zzzz"));
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
                    List<Map<String, String>> drivers = dbHandler.collectJdbcDrivers();

                    evt.setPayload(toJson(drivers));

                    webSocketSes.send(evt);

                    break;

                case SCHEMA_IMPORT_SCHEMAS:
                    log.info(SCHEMA_IMPORT_SCHEMAS);
                    break;

                case SCHEMA_IMPORT_METADATA:
                    log.info(SCHEMA_IMPORT_METADATA);
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

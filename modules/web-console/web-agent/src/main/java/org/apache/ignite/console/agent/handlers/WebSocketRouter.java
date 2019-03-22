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

import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;

/**
 * Router that listen for web socket and redirect messages to event bus.
 */
public class WebSocketRouter {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(WebSocketRouter.class));

    /** */
    private static final String AGENT_ID = UUID.randomUUID().toString();

//    /** */
//    private static final Buffer PING = new JsonObject()
//        .put("address", Addresses.INFO)
//        .put("type", "ping")
//        .put("body", "ping")
//        .toBuffer();

    /** */
    private final AgentConfiguration cfg;

//    /** */
//    private HttpClient client;

    /** */
    private volatile long curTimer;

//    /** */
//    private RequestOptions conOpts;

    /**
     * @param cfg Configuration.
     */
    public WebSocketRouter(AgentConfiguration cfg) {
        this.cfg = cfg;
    }


//    /** {@inheritDoc} */
//    @Override public void start() throws Exception {
//        log.info("Web Agent ID: " + AGENT_ID);
//        log.info("Connecting to: " + cfg.serverUri());
//
//        boolean serverTrustAll = Boolean.getBoolean("trust.all");
//
//        if (serverTrustAll && !F.isEmpty(cfg.serverTrustStore())) {
//            log.warning("Options contains both '--server-trust-store' and '-Dtrust.all=true'. " +
//                "Option '-Dtrust.all=true' will be ignored on connect to Web server.");
//
//            serverTrustAll = false;
//        }
//
//        HttpClientOptions httpOptions = new HttpClientOptions()
//            .setTryUseCompression(true)
//            .setTryUsePerMessageWebsocketCompression(true);
//
//        boolean ssl = serverTrustAll || !F.isEmpty(cfg.serverKeyStore()) || !F.isEmpty(cfg.serverTrustStore());
//
//        if (ssl) {
//            httpOptions.setSsl(true);
//
//            JksOptions jks = jksOptions(cfg.serverKeyStore(), cfg.serverKeyStorePassword());
//
//            if (jks != null)
//                httpOptions.setKeyStoreOptions(jks);
//
//            if (serverTrustAll) {
//                httpOptions
//                    .setTrustAll(true)
//                    .setVerifyHost(false);
//            }
//            else {
//                jks = jksOptions(cfg.serverTrustStore(), cfg.serverTrustStorePassword());
//
//                if (jks != null)
//                    httpOptions.setTrustStoreOptions(jks);
//            }
//
//            if (!F.isEmpty(cfg.cipherSuites()))
//                cfg.cipherSuites().forEach(httpOptions::addEnabledCipherSuite);
//        }
//
//        client = vertx.createHttpClient(httpOptions);
//
//        URI uri = new URI(cfg.serverUri());
//
//        conOpts = new RequestOptions()
//            .setHost(uri.getHost())
//            .setPort(uri.getPort())
//            .setSsl(ssl)
//            .setURI("/eventbus/websocket");
//
//        curTimer = vertx.setTimer(1, this::connect);
//    }
//
//    /** {@inheritDoc} */
//    @Override public void stop() {
//        vertx.cancelTimer(curTimer);
//
//        client.close();
//    }

//    /**
//     * Write data to web socket.
//     * @param ws Web socket.
//     * @param data Data to send over socket.
//     */
//    private void write(WebSocket ws, Buffer data) {
//        ws.writeBinaryMessage(data);
//    }
//
//    /**
//     * Register consumer on event bus.
//     *
//     * @param ws Web socket.
//     * @param addr Consumer address on event bus.
//     */
//    private void register(WebSocket ws, String addr) {
//        JsonObject json = new JsonObject()
//            .put("type", "register")
//            .put("address", addr)
//            .put("headers", "{}");
//
//        write(ws, buffer(json.encode()));
//    }
//
//    /**
//     * Send message to event bus.
//     *
//     * @param ws Web socket.
//     * @param addr Address on event bus.
//     * @param data Data to send.
//     */
//    private void send(WebSocket ws, String addr, Object data) {
//        try {
//            JsonObject json = new JsonObject()
//                .put("address", addr)
//                .put("type", "send")
//                .put("body", data);
//
//            write(ws, buffer(json.encode()));
//        }
//        catch (Throwable e) {
//            log.error("Failed to send message to address: " + addr, e);
//        }
//    }
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
}

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
import java.util.UUID;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;

import static org.apache.ignite.console.agent.handlers.Addresses.EVENT_CLUSTER_TOPOLOGY;
import static org.apache.ignite.console.agent.handlers.Addresses.EVENT_SCHEMA_IMPORT_DRIVERS;

/**
 * Router that listen for web socket and redirect messages to event bus.
 */
public class WebSocketRouter extends AbstractVerticle {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(WebSocketRouter.class));

    /** */
    private static final JsonObject AGENT_ID = new JsonObject().put("agentId", UUID.randomUUID().toString());

    /** */
    private static final Buffer PING = new JsonObject()
        .put("address", "info")
        .put("type", "ping")
        .put("body", "ping")
        .toBuffer();

    /** */
    private final AgentConfiguration cfg;


    /** */
    private HttpClient client;

    /** */
    private volatile long curTimer;

    /** */
    private RequestOptions conOpts;

    /**
     * @param cfg Configuration.
     */
    public WebSocketRouter(AgentConfiguration cfg) {
        this.cfg = cfg;
    }


    /** {@inheritDoc} */
    @Override public void start() throws Exception {
        log.info("Web Agent: " + AGENT_ID);
        log.info("Connecting to: " + cfg.serverUri());

        boolean serverTrustAll = Boolean.getBoolean("trust.all");
        boolean hasServerTrustStore = cfg.serverTrustStore() != null;

        if (serverTrustAll && hasServerTrustStore) {
            log.warning("Options contains both '--server-trust-store' and '-Dtrust.all=true'. " +
                "Option '-Dtrust.all=true' will be ignored on connect to Web server.");

            serverTrustAll = false;
        }

        HttpClientOptions httpOptions = new HttpClientOptions();

        boolean ssl = serverTrustAll || hasServerTrustStore || cfg.serverKeyStore() != null;

        if (ssl) {
            httpOptions
                .setSsl(true)
                .setTrustAll(serverTrustAll)
                .setKeyStoreOptions(new JksOptions()
                    .setPath(cfg.serverKeyStore())
                    .setPassword(cfg.serverKeyStorePassword()))
                .setTrustStoreOptions(new JksOptions()
                    .setPath(cfg.serverTrustStore())
                    .setPassword(cfg.serverTrustStorePassword()));

            if (!F.isEmpty(cfg.cipherSuites()))
                cfg.cipherSuites().forEach(httpOptions::addEnabledCipherSuite);
        }

        client = vertx.createHttpClient(httpOptions);

        URI uri = new URI(cfg.serverUri());

        conOpts = new RequestOptions()
            .setHost(uri.getHost())
            .setPort(uri.getPort())
            .setSsl(ssl)
            .setURI("/eventbus/websocket");

        curTimer = vertx.setTimer(1, this::connect);
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        vertx.cancelTimer(curTimer);

        client.close();
    }

    /**
     * Send message to event bus.
     *
     * @param ws Web socket.
     * @param addr Address on event bus.
     * @param data Data to send.
     */
    private void send(WebSocket ws, String addr, JsonObject data) {
        JsonObject json = new JsonObject()
            .put("type", "send")
            .put("address", addr)
            .put("body", data);

        ws.write(json.toBuffer());
    }

    /**
     * Send message to event bus.
     *
     * @param ws Web socket.
     * @param addr Address to register.
     */
    private void register(WebSocket ws, String addr) {
        JsonObject json = new JsonObject()
            .put("type", "register")
            .put("address", addr)
            .put("headers", "{}");

        ws.write(json.toBuffer());
    }

    /**
     * Connect to server.
     *
     * @param tid Timer ID.
     */
    private void connect(long tid) {
//        vertx.eventBus().consumer(EVENT_CLUSTER_TOPOLOGY, msg -> {
//            log.info(EVENT_CLUSTER_TOPOLOGY + msg.body());
//        });

        client.websocket(conOpts,
            ws -> {
                log.info("Connected to server: " + ws.remoteAddress());

                register(ws, EVENT_SCHEMA_IMPORT_DRIVERS);

                ws.handler(data -> {
                    JsonObject json = data.toJsonObject();

                    String addr = json.getString("address");

                    if (F.isEmpty(addr))
                        log.error("Unexpected request: " + json);
                    else
                        vertx.eventBus().send(addr, json.getJsonObject("data"), msg -> {
                            if (msg.failed())
                                log.error("Failed to process: " + json + ", reason: " + msg.cause().getMessage());
                            else {
                                String res = String.valueOf(msg.result().body());

                                send();
                                ws.writeTextMessage(res);
                            }
                        });

                    log.info("Received data " + data.toString());
                });

                long pingTimer = vertx.setPeriodic(3000, v -> ws.write(PING));

                ws.closeHandler(p -> {
                    log.warning("Connection closed: " + ws.remoteAddress());

                    vertx.cancelTimer(pingTimer);

                    curTimer = vertx.setTimer(1, this::connect);
                });


                send(ws, "agent:id", AGENT_ID);
            },
            e -> {
                LT.warn(log, e.getMessage());

                curTimer = vertx.setTimer(3000, this::connect);
            });
    }
}

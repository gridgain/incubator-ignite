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
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;

/**
 * Router that listen for web socket and redirect messages to event bus.
 */
public class WebSocketRouter extends AbstractVerticle {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(WebSocketRouter.class));

    /** */
    private final AgentConfiguration cfg;

    /** */
    private final JsonObject agentId;

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

        agentId = new JsonObject();
        agentId.put("agentId", UUID.randomUUID().toString());
    }


    /** {@inheritDoc} */
    @Override public void start() throws Exception {
        log.info("Web Agent: " + agentId);
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
            .setURI("/web-agents");

        curTimer = vertx.setTimer(1, this::connect);
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        vertx.cancelTimer(curTimer);

        client.close();
    }

    /**
     * Connect to server.
     *
     * @param tid Timer ID.
     */
    private void connect(long tid) {
        client.websocket(conOpts,
            ws -> {
                log.info("Connected to server: " + ws.remoteAddress());

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
                                String response = String.valueOf(msg.result().body());

                                ws.writeTextMessage(response);
                            }
                        });

                    log.info("Received data " + data.toString());
                });

                ws.closeHandler(p -> {
                    log.warning("Connection closed: " + ws.remoteAddress());

                    curTimer = vertx.setTimer(1, this::connect);
                });

                ws.writeTextMessage(agentId.toString());
            },
            e -> {
                LT.warn(log, e.getMessage());

                curTimer = vertx.setTimer(3000, this::connect);
            });
    }
}

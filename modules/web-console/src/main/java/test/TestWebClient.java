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

package test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class TestWebClient extends AbstractVerticle {
    /** */
    private static final SimpleDateFormat DEBUG_DATE_FMT = new SimpleDateFormat("HH:mm:ss,SSS");

    /** */
    private static String AGENT_ID = UUID.randomUUID().toString();

    /** */
    private HttpClient client;

    /** */
    private static void log(Object s) {
        System.out.println('[' + DEBUG_DATE_FMT.format(new Date(System.currentTimeMillis())) + "] " +
            "[" + Thread.currentThread().getName() + ']' + ' ' + s);
    }

    /** */
    public static void main(String... args) {
        log("Starting client...");

        Vertx.vertx().deployVerticle(new TestWebClient());

        log("Started client!");
    }

    /** {@inheritDoc} */
    @Override public void start() {
        client = vertx.createHttpClient();

        vertx.setTimer(1, this::connect);
    }

    /**
     * Connect to server.
     *
     * @param tid Timer ID.
     */
    private void connect(long tid) {
        RequestOptions conOpts = new RequestOptions()
            .setHost("localhost")
            .setPort(3000)
            .setURI("/eventbus/websocket");

        client.websocket(conOpts,
            ws -> {
                log("Connected to server: " + ws.remoteAddress());

                ws.handler(data -> {
                    JsonObject json = data.toJsonObject();

                    String addr = json.getString("address");

                    if (F.isEmpty(addr))
                        log("Unexpected request: " + json);
                    else
                        vertx.eventBus().send(addr, json.getJsonObject("data"), msg -> {
                            if (msg.failed())
                                log("Failed to process: " + json + ", reason: " + msg.cause().getMessage());
                            else {
                                String res = String.valueOf(msg.result().body());

                                ws.writeTextMessage(res);
                            }
                        });

                    log("Received data " + data.toString());
                });

                ws.closeHandler(p -> {
                    log("Connection closed: " + ws.remoteAddress());

                    vertx.setTimer(1, this::connect);
                });

                JsonObject agentId = new JsonObject()
                    .put("agentId", AGENT_ID);

                JsonObject json = new JsonObject()
                    .put("type", "send")
                    .put("address", "agent:info")
                    .put("body", agentId);

                ws.write(json.toBuffer());
            },
            e -> {
                log(e.getMessage());

                vertx.setTimer(3000, this::connect);
            });
    }

}

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

package org.apache.ignite.web.console;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.Session;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CookieHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.handler.UserSessionHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.sstore.LocalSessionStore;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.web.console.auth.IgniteAuth;

import static io.vertx.core.http.HttpMethod.GET;
import static io.vertx.core.http.HttpMethod.POST;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

/**
 * Handler
 */
public class WebConsoleHttpServerVerticle extends AbstractVerticle {
    /** */
    private static final SimpleDateFormat DEBUG_DATE_FMT = new SimpleDateFormat("HH:mm:ss,SSS");

    /** */
    private Ignite ignite;

    /** */
    private IgniteAuth auth;

    /** */
    private SockJSHandler browsersHandler;

    /** */
    private Map<ServerWebSocket, String> agentSockets = new ConcurrentHashMap<>();

    /** */
    private Map<ServerWebSocket, JsonObject> agentResponses = new ConcurrentHashMap<>();

    /**
     * @param s Message to log.
     */
    private void log(Object s) {
        System.out.println('[' + DEBUG_DATE_FMT.format(new Date(System.currentTimeMillis())) + "] [" +
            Thread.currentThread().getName() + ']' + ' ' + s);
    }

    /**
     * Start Ignite.
     */
    private void startIgnite() throws SQLException {
        long start = System.currentTimeMillis();

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName("Web Console backend");
        cfg.setMetricsLogFrequency(0);
        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi discovery = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500"));

        discovery.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discovery);

        DataStorageConfiguration dataStorageCfg = new DataStorageConfiguration();

        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration();

        // dataRegionCfg.setPersistenceEnabled(true);

        dataStorageCfg.setDefaultDataRegionConfiguration(dataRegionCfg);

        cfg.setDataStorageConfiguration(dataStorageCfg);

        ignite = Ignition.getOrStart(cfg);

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache("users");
        cache.put("kuaw26@mail.ru", "1");

        log("Ignite started: " + (System.currentTimeMillis() - start));
    }

    /**
     * @param bopts TODO
     * @param addr TODO
     */
    private void bind(BridgeOptions bopts, String addr) {
        bopts
            .addInboundPermitted(new PermittedOptions().setAddress(addr))
            .addOutboundPermitted(new PermittedOptions().setAddress(addr));
    }

    /** {@inheritDoc} */
    @Override public void start() throws Exception {
        startIgnite();

        long start = System.currentTimeMillis();

        browsersHandler = SockJSHandler.create(vertx);

        BridgeOptions bopts = new BridgeOptions();

        bind(bopts, "agent:stats");
        bind(bopts, "browser:info");
        bind(bopts, "node:rest");
        bind(bopts, "node:visor");
        bind(bopts, "schemaImport:drivers");
        bind(bopts, "schemaImport:schemas");
        bind(bopts, "schemaImport:metadata");

        browsersHandler.bridge(bopts);

        EventBus eventBus = vertx.eventBus();

        eventBus.consumer("browser:info", msg -> {
            log("browser:info: " + msg.address() + " " + msg.body());
        });

        eventBus.consumer("node:rest", msg -> log(msg.body()));

        eventBus.consumer("node:visor", msg -> log(msg.body()));

        eventBus.consumer("schemaImport:drivers", msg -> {
            log("schemaImport:drivers: " +  msg.body());

            vertx.executeBlocking(
                fut -> {
                    ServerWebSocket sock = agentSockets.keySet().iterator().next();
                    JsonObject json = new JsonObject();
                    json.put("address", "schemaImport:drivers");
                    sock.writeTextMessage(json.toString());

                    JsonObject res = agentResponses.get(sock);

                    while(res == null) {
                        try {
                            Thread.sleep(10);
                        }
                        catch (InterruptedException e) {
                            fut.fail(e);
                        }

                        res = agentResponses.get(sock);
                    }

                    fut.complete(res);
                },
                async -> {
                    msg.reply(async.result());
                });
        });

        eventBus.consumer("schemaImport:schemas", msg -> log(msg.body()));

        eventBus.consumer("schemaImport:metadata", msg -> log(msg.body()));

        // Create a router object.
        Router router = Router.router(vertx);

        auth = IgniteAuth.create(vertx, ignite);

        // We need cookies, sessions and request bodies.
        router.route().handler(CookieHandler.create());
        router.route().handler(BodyHandler.create());
        router.route().handler(SessionHandler.create(LocalSessionStore.create(vertx)));

        // We need a user session handler too to make sure the user is stored in the session between requests.
        router.route().handler(UserSessionHandler.create(auth));

        router.route().handler(CorsHandler.create(".*")
            .allowedMethod(GET)
            .allowedMethod(POST)
            .allowCredentials(true)
            .allowedHeader("Access-Control-Allow-Method")
            .allowedHeader("Access-Control-Allow-Origin")
            .allowedHeader("Access-Control-Allow-Credentials")
            .allowedHeader("Content-Type"));

        router.route("/browsers/*").handler(browsersHandler);

        router.route("/api/v1/user").handler(this::handleUser);
        router.route("/api/v1/signup").handler(this::handleSignUp);
        router.route("/api/v1/signin").handler(this::handleSignIn);
        router.route("/api/v1/logout").handler(this::handleLogout);
        router.route("/api/v1/password/forgot").handler(this::handlePasswordForgot);
        router.route("/api/v1/password/reset").handler(this::handlePasswordReset);
        router.route("/api/v1/password/validate/token").handler(this::handlePasswordValidateToken);
        router.route("/api/v1/activation/resend").handler(this::handleActivationResend);

        router.route("/api/v1/configuration/clusters").handler(this::handleClusters);

        router.route("/api/v1/activities/page").handler(this::handleActivitiesPage);

        /*
            app.use('/api/v1/admin', _mustAuthenticated, _adminOnly, adminRoute);
            app.use('/api/v1/profile', _mustAuthenticated, profilesRoute);
            app.use('/api/v1/demo', _mustAuthenticated, demoRoute);

            app.all('/api/v1/configuration/*', _mustAuthenticated);

            app.use('/api/v1/configuration/clusters', clustersRoute);
            app.use('/api/v1/configuration/domains', domainsRoute);
            app.use('/api/v1/configuration/caches', cachesRoute);
            app.use('/api/v1/configuration/igfs', igfssRoute);
            app.use('/api/v1/configuration', configurationsRoute);

            app.use('/api/v1/notebooks', _mustAuthenticated, notebooksRoute);
            app.use('/api/v1/downloads', _mustAuthenticated, downloadsRoute);
            app.use('/api/v1/activities', _mustAuthenticated, activitiesRoute);

            '/api/v1/signin'
            '/api/v1/configuration/clusters'

         */

//        vertx.setPeriodic(1000L, t -> agentSockets.forEach((key, value) -> {
//            log("Send message to agent: " + value);
//
//            JsonObject json = new JsonObject();
//            json.put("address", "schemaImport:drivers");
//
//            key.writeTextMessage(json.toString());
//        }));

        vertx.setPeriodic(3000L, t -> {
            JsonObject json = new JsonObject();
            json.put("count", agentSockets.size());
            json.put("hasDemo", false);
            json.put("clusters", new JsonArray());

            eventBus.send("agent:stats", json);
        });

        router.route("/web-agents/*").handler(ctx -> {
            log("/web-agents/*");
        });

        // Create the HTTP server for browsers.
        vertx
            .createHttpServer()
            .requestHandler(router)
            //.websocketHandler(this::webSocketHandler)
            .listen(3000);

        log("Web Server started: " + (System.currentTimeMillis() - start));
    }

    /**
     * @param status Status to send.
     * @param ctx Context.
     */
    private void sendStatus(int status, RoutingContext ctx) {
        ctx.response().setStatusCode(status).end();
    }

    /**
     * @param json JSON object to send.
     * @param ctx Context.
     */
    private void sendJson(JsonObject json, RoutingContext ctx) {
        ctx.response()
            .putHeader("content-type", "application/json")
            .end(Json.encode(json));
    }

    /**
     * @param ws Web socket.
     */
    private void webSocketHandler(ServerWebSocket ws) {
        log("webSocketHandler2: " + ws.path());

        ws.handler(buf -> {
            JsonObject msg = buf.toJsonObject();

            log("Received via WebSocket: " + msg);

            String agent = msg.getString("agentId");

            if (agent != null)
                agentSockets.putIfAbsent(ws, agent);
            else
                agentResponses.put(ws, msg);
        });

        ws.closeHandler(p -> {
            log("Socket closed for agent: " + agentSockets.get(ws));

            agentSockets.remove(ws);
        });
    }

    /**
     * @param ctx Context
     */
    private void handleUser(RoutingContext ctx) {
        User user = ctx.user();

        if (user == null)
            sendStatus(HTTP_UNAUTHORIZED, ctx);
        else
            sendJson(user.principal(), ctx);
    }

    /**
     * @param ctx Context
     */
    private void handleSignUp(RoutingContext ctx) {
        JsonObject body = ctx.getBodyAsJson();

        auth.authenticate(body, asynRes -> {
            if (asynRes.succeeded())
                ctx.response().setStatusCode(HTTP_OK);

        });

        HttpServerResponse res = ctx.response();

        res.
            setStatusCode(HTTP_OK);
    }

    /**
     * @param ctx Context
     */
    private void handleSignIn(RoutingContext ctx) {
        log("SignIn");

        JsonObject data = ctx.getBody().toJsonObject();

        log("SignIn: " + data);

        IgniteCache<String, String> users = ignite.cache("users");
        String user = users.get(data.getString("email"));

        HttpServerResponse res = ctx.response();

        if (user == null)
            res.setStatusCode(HTTP_UNAUTHORIZED).end("Invalid email or password");
        else {
            logged = true;

            res.setStatusCode(HTTP_OK).end();
        }
    }

    /**
     * @param ctx Context
     */
    private void handleLogout(RoutingContext ctx) {
        log(ctx.request().uri());

        logged = false;

        HttpServerResponse res = ctx.response();

        res.setStatusCode(HTTP_OK).end();
    }

    /**
     * @param ctx Context
     */
    private void handlePasswordForgot(RoutingContext ctx) {
        log(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("{\"_id\":\"5683a8e9824d152c044e6281\"}");
    }

    /**
     * @param ctx Context
     */
    private void handlePasswordReset(RoutingContext ctx) {
        log(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("{\"_id\":\"5683a8e9824d152c044e6281\"}");
    }

    /**
     * @param ctx Context
     */
    private void handlePasswordValidateToken(RoutingContext ctx) {
        log(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("{\"_id\":\"5683a8e9824d152c044e6281\"}");
    }

    /**
     * @param ctx Context
     */
    private void handleActivationResend(RoutingContext ctx) {
        log(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("{\"_id\":\"5683a8e9824d152c044e6281\"}");
    }

    /**
     * @param ctx Context
     */
    private void handleClusters(RoutingContext ctx) {
        log(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("[{\"_id\":\"5b9b5ad477670d001936692a\",\"name\":\"LOST_AND_FOUND\",\"discovery\":\"Multicast\",\"cachesCount\":0,\"modelsCount\":0,\"igfsCount\":0,\"gridgainInfo\":{\"gridgainEnabled\":true,\"rollingUpdatesEnabled\":true,\"securityEnabled\":true,\"dataReplicationReceiverEnabled\":false,\"dataReplicationSenderEnabled\":false,\"snapshotsEnabled\":false}},{\"_id\":\"5c340a488438de7b2eb2a4ba\",\"name\":\"Cluster1\",\"discovery\":\"Multicast\",\"cachesCount\":0,\"modelsCount\":0,\"igfsCount\":0,\"gridgainInfo\":{\"gridgainEnabled\":false,\"rollingUpdatesEnabled\":false,\"securityEnabled\":false,\"dataReplicationReceiverEnabled\":false,\"dataReplicationSenderEnabled\":false,\"snapshotsEnabled\":false}}]");
    }

    /**
     * @param ctx Context
     */
    private void handleActivitiesPage(RoutingContext ctx) {
        log(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("{\"_id\":\"5c340a44b46d730801ad839f\",\"action\":\"base.configuration.overview\",\"date\":\"2019-01-01T00:00:00.000Z\",\"owner\":\"5683a8e9824d152c044e6281\",\"__v\":0,\"group\":\"configuration\",\"amount\":4}");
    }
}

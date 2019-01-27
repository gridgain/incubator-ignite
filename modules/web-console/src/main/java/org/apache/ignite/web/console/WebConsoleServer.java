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

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CookieHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.handler.UserSessionHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.sstore.LocalSessionStore;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.web.console.auth.IgniteAuth;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Web Console server.
 */
@SuppressWarnings("JavaAbbreviationUsage")
public class WebConsoleServer extends AbstractVerticle {
    /** */
    private static final SimpleDateFormat DEBUG_DATE_FMT = new SimpleDateFormat("HH:mm:ss,SSS");

    /** */
    private static final String VISOR_IGNITE = "org.apache.ignite.internal.visor.";

    /** */
    private static final String VISOR_GRIDGAIN = "org.gridgain.grid.internal.visor.";

    /** */
    private static final String VISOR_SNAPSHOT = "org.gridgain.grid.internal.visor.database.snapshot.";

    /** */
    private static final String VISOR_DR = "org.gridgain.grid.internal.visor.dr.";

    /** */
    private Ignite ignite;

    /** */
    private IgniteAuth auth;

    /** */
    private Map<String, JsonObject> clusters = new ConcurrentHashMap<>();

    /** */
    private boolean embedded;

    /**
     * @param s Message to log.
     */
    private static void log(Object s) {
        System.out.println("[" + DEBUG_DATE_FMT.format(new Date(System.currentTimeMillis())) + "] [" +
            Thread.currentThread().getName() + "]" + " " + s);
    }

    /**
     * @param embedded Whether Web Console run in embedded mode.
     */
    public WebConsoleServer(boolean embedded) {
        this.embedded = embedded;
    }

    /**
     * Start Ignite.
     */
    private void startIgnite() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName("Web Console backend");
        cfg.setConsistentId("web-console-backend");
        cfg.setMetricsLogFrequency(0);
        cfg.setLocalHost("127.0.0.1");

        cfg.setWorkDirectory(new File(U.getIgniteHome(), "work-web-console").getAbsolutePath());

        TcpDiscoverySpi discovery = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:60800"));

        discovery.setLocalPort(60800);
        discovery.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discovery);

        DataStorageConfiguration dataStorageCfg = new DataStorageConfiguration();

        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration();

        dataRegionCfg.setPersistenceEnabled(true);

        dataStorageCfg.setDefaultDataRegionConfiguration(dataRegionCfg);

        cfg.setDataStorageConfiguration(dataStorageCfg);

        CacheConfiguration accountsCfg = new CacheConfiguration(Consts.ACCOUNTS_CACHE_NAME);
        accountsCfg.setCacheMode(REPLICATED);

        CacheConfiguration notebooksCfg = new CacheConfiguration(Consts.NOTEBOOKS_CACHE_NAME);
        accountsCfg.setCacheMode(REPLICATED);

        cfg.setCacheConfiguration(accountsCfg, notebooksCfg);

        cfg.setConnectorConfiguration(null);

        ignite = Ignition.getOrStart(cfg);

        ignite.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override public void start() {
        long start = System.currentTimeMillis();

        log("Embedded mode: " + embedded);

        startIgnite();

        SockJSHandler sockJsHnd = SockJSHandler.create(vertx);

        BridgeOptions allAccessOptions =
            new BridgeOptions()
                .addInboundPermitted(new PermittedOptions())
                .addOutboundPermitted(new PermittedOptions());

        sockJsHnd.bridge(allAccessOptions);

        EventBus eventBus = vertx.eventBus();

//        eventBus.consumer("agent:id", msg -> log(msg.address() + " " + msg.body()));
//        eventBus.consumer("browser:info", msg -> log(msg.address() + " " + msg.body()));
        eventBus.consumer("cluster:topology", this::handleClusterTopology);

        // Create a router object.
        Router router = Router.router(vertx);

        auth = IgniteAuth.create(vertx, ignite);

        router.route().handler(CookieHandler.create());
        router.route().handler(BodyHandler.create());
        router.route().handler(SessionHandler.create(LocalSessionStore.create(vertx)));
        router.route().handler(UserSessionHandler.create(auth));

        router.route("/eventbus/*").handler(sockJsHnd);

        registerRestRoutes(router);

//        eventBus.addOutboundInterceptor(ctx -> {
//            Message<Object> msg = ctx.message();
//
//            log("interceptor" + msg.address() + msg.body());
//        });

        // Start HTTP server for browsers and web agents.
        HttpServerOptions httpOpts = new HttpServerOptions()
            .setCompressionSupported(true)
            .setPerMessageWebsocketCompressionSupported(true);

        vertx
            .createHttpServer(httpOpts)
            .requestHandler(router)
            .listen(3000);

        log("Web Console started: " + (System.currentTimeMillis() - start));
    }

    /**
     * Register REST routes.
     *
     * @param router Router.
     */
    private void registerRestRoutes(Router router) {
        router.route("/api/v1/user").handler(this::handleUser);
        router.route("/api/v1/signup").handler(this::handleSignUp);
        router.route("/api/v1/signin").handler(this::handleSignIn);
        router.route("/api/v1/logout").handler(this::handleLogout);
        router.route("/api/v1/password/forgot").handler(this::handleDummy);
        router.route("/api/v1/password/reset").handler(this::handleDummy);
        router.route("/api/v1/password/validate/token").handler(this::handleDummy);
        router.route("/api/v1/activation/resend").handler(this::handleDummy);
        router.route("/api/v1/activities/page").handler(this::handleDummy);
        router.route("/api/v1/admin").handler(this::handleDummy);
        router.route("/api/v1/profile").handler(this::handleDummy);
        router.route("/api/v1/demo").handler(this::handleDummy);
        router.route("/api/v1/configuration/clusters").handler(this::handleDummy);
        router.route("/api/v1/configuration/domains").handler(this::handleDummy);
        router.route("/api/v1/configuration/caches").handler(this::handleDummy);
        router.route("/api/v1/configuration/igfs").handler(this::handleDummy);
        router.route("/api/v1/configuration").handler(this::handleDummy);
        router.get("/api/v1/notebooks").handler(this::handleNotebooks);
        router.post("/api/v1/notebooks/save").handler(this::handleNotebookSave);
        router.post("/api/v1/notebooks/delete").handler(this::handleNotebookDelete);
        router.route("/api/v1/downloads").handler(this::handleDummy);
        router.route("/api/v1/activities").handler(this::handleDummy);
    }

    /**
     * @param shortName Class short name.
     * @return Full class name.
     */
    private String igniteVisor(String shortName) {
        return VISOR_IGNITE + shortName;
    }

    /**
     * @param shortName Class short name.
     * @return Full class name.
     */
    private String gridgainVisor(String shortName) {
        return VISOR_GRIDGAIN + shortName;
    }

    /**
     * @param shortName Class short name.
     * @return Full class name.
     */
    private String snapshotsVisor(String shortName) {
        return VISOR_SNAPSHOT + shortName;
    }

    /**
     * @param shortName Class short name.
     * @return Full class name.
     */
    private String drVisor(String shortName) {
        return VISOR_DR + shortName;
    }

    /**
     * @param ctx Context.
     * @param status Status to send.
     */
    private void sendStatus(RoutingContext ctx, int status) {
        ctx.response().setStatusCode(status).end();
    }

    /**
     * @param ctx Context.
     * @param status Status to send.
     * @param msg Message to send.
     */
    private void sendStatus(RoutingContext ctx, int status, String msg) {
        ctx.response().setStatusCode(status).end(msg);
    }

    /**
     * @param cause Error.
     * @return Error message or exception class name.
     */
    private String errorMessage(Throwable cause) {
        String msg = cause.getMessage();

       return F.isEmpty(msg) ? cause.getClass().getName() : msg;
    }

    /**
     * @param json JSON object to send.
     * @param ctx Context.
     */
    private void sendJson(JsonObject json, RoutingContext ctx) {
        ctx.response()
            .putHeader("content-type", "application/json; charset=UTF-8")
            .end(Json.encode(json));
    }

    /**
     * @param ctx Context
     */
    private void handleUser(RoutingContext ctx) {
        User user = ctx.user();

        if (user == null)
            sendStatus(ctx, HTTP_UNAUTHORIZED);
        else
            sendJson(user.principal(), ctx);
    }

    /**
     * @param ctx Context
     */
    private void handleSignUp(RoutingContext ctx) {
        JsonObject authInfo = ctx.getBodyAsJson();

        authInfo.put("signup", true);

        auth.authenticate(authInfo, asyncRes -> {
            if (asyncRes.succeeded())
                sendStatus(ctx, HTTP_OK);
            else
                sendStatus(ctx, HTTP_UNAUTHORIZED, errorMessage(asyncRes.cause()));
        });
    }

    /**
     * @param ctx Context
     */
    private void handleSignIn(RoutingContext ctx) {
        auth.authenticate(ctx.getBody().toJsonObject(), asyncRes -> {
            if (asyncRes.succeeded()) {
                ctx.setUser(asyncRes.result());

                sendStatus(ctx, HTTP_OK);
            }
            else
                sendStatus(ctx, HTTP_UNAUTHORIZED, errorMessage(asyncRes.cause()));
        });
    }

    /**
     * @param ctx Context
     */
    private void handleLogout(RoutingContext ctx) {
        ctx.clearUser();

        sendStatus(ctx, HTTP_OK);
    }

    /**
     * @param ctx Context
     */
    private void handleNotebooks(RoutingContext ctx) {
        sendStatus(ctx, HTTP_OK, "[]");
    }

    /**
     * @param ctx Context
     */
    private void handleNotebookSave(RoutingContext ctx) {
        sendStatus(ctx, HTTP_OK, "[]");
    }

    /**
     * @param ctx Context
     */
    private void handleNotebookDelete(RoutingContext ctx) {
        sendStatus(ctx, HTTP_OK, "[]");
    }

    /**
     * @param ctx Context
     */
    private void handleDummy(RoutingContext ctx) {
        sendStatus(ctx, HTTP_OK, "[]");
    }

    /**
     * Handle cluster topology event.
     *
     * @param msg Message with data.
     */
    private void handleClusterTopology(Message<JsonObject> msg) {
        JsonObject data = msg.body();

        String agentId = data.getString("agentId");

        JsonObject newTop = data.getJsonObject("top");

        JsonObject oldTop = clusters.get(agentId);

        // TODO IGNITE-5617 Implement better detection of changed cluster
        JsonArray oldNids = oldTop != null ? oldTop.getJsonArray("nids") : new JsonArray();
        JsonArray newNids = newTop.getJsonArray("nids");

        if (!oldNids.equals(newNids)) {
            newTop.put("id", UUID.randomUUID().toString()); // TODO IGNITE-5617 quick hack for prototype.

            clusters.put(agentId, newTop);

            JsonObject json = new JsonObject()
                .put("count", clusters.size())
                .put("hasDemo", false)
                .put("clusters", new JsonArray().add(newTop)); // TODO IGNITE-5617 quick hack for prototype.

            vertx.eventBus().send("agents:stat", json);
        }
    }

    /**
     * Main entry point.
     *
     * @param args Arguments.
     */
    public static void main(String... args) {
        log("Web Console starting...");

        Vertx.vertx(new VertxOptions()
            .setBlockedThreadCheckInterval(1000 * 60 * 60))
            .deployVerticle(new WebConsoleServer(false));
    }
}

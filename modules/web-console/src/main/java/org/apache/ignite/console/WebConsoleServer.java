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

package org.apache.ignite.console;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.bridge.BridgeEventType;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CookieHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.handler.sockjs.BridgeEvent;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.sstore.ClusteredSessionStore;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.common.Addresses;
import org.apache.ignite.console.routes.RestApiRouter;
import org.apache.ignite.internal.util.typedef.F;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.ignite.console.common.Utils.errorMessage;

/**
 * Web Console server.
 */
public class WebConsoleServer extends AbstractVerticle {
    /** */
    private static final String VISOR_IGNITE = "org.apache.ignite.internal.visor.";

    /** */
    private static final List<CharSequence> HTTP_CACHE_CONTROL = Arrays.asList(
        HttpHeaderValues.NO_CACHE,
        HttpHeaderValues.NO_STORE,
        HttpHeaderValues.MUST_REVALIDATE);

    /** */
    protected final Map<String, VisorTaskDescriptor> visorTasks = new ConcurrentHashMap<>();

    /** */
    protected final Map<String, JsonObject> clusters = new ConcurrentHashMap<>();

    /** */
    protected final Ignite ignite;

    /** */
    private final RestApiRouter[] routers;

    /**
     * @param ignite Ignite.
     * @param routers REST API routers.
     */
    public WebConsoleServer(
        Ignite ignite,
        RestApiRouter... routers
    ) {
        this.ignite = ignite;
        this.routers = routers;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        SockJSHandler sockJsHnd = SockJSHandler.create(vertx);

        BridgeOptions allAccessOptions =
            new BridgeOptions()
                .addInboundPermitted(new PermittedOptions())
                .addOutboundPermitted(new PermittedOptions());

        sockJsHnd.bridge(allAccessOptions, this::handleNodeVisorMessages);

        registerEventBusConsumers();

        Router router = Router.router(vertx);

        router.route().handler(CookieHandler.create());
        router.route().handler(BodyHandler.create());
        router.route().handler(SessionHandler.create(ClusteredSessionStore.create(vertx)));

        router.route("/eventbus/*").handler(sockJsHnd);

        registerRestRoutes(router);

        registerVisorTasks();

        // Start HTTP server for browsers and web agents.
        HttpServerOptions httpOpts = new HttpServerOptions()
            .setCompressionSupported(true)
            .setPerMessageWebsocketCompressionSupported(true);

        vertx
            .createHttpServer(httpOpts)
            .requestHandler(router)
            .listen(3000);

        ignite.log().info("Web Console server started.");
    }

    /**
     * Register event bus consumers.
     */
    protected void registerEventBusConsumers() {
        EventBus evtBus = vertx.eventBus();

        evtBus.consumer(Addresses.CLUSTER_TOPOLOGY, this::handleClusterTopology);

        vertx.setPeriodic(3000, this::refreshTop);
    }

    /**
     * @param tid Timer ID.
     */
    @SuppressWarnings("unused")
    private void refreshTop(long tid) {
        JsonObject json = new JsonObject()
            .put("count", 1)
            .put("hasDemo", false);

        JsonArray zz = new JsonArray(); // TODO IGNITE-5617 temporary hack

        clusters.forEach((k, v) -> zz.add(v));

        json.put("clusters", zz);

        vertx.eventBus().send(Addresses.AGENTS_STATUS, json);
    }

    /**
     * Register REST routes.
     *
     * @param router Router.
     */
    private void registerRestRoutes(Router router) {
        router.route("/api/v1/activation/resend").handler(this::handleDummy);
        router.route("/api/v1/activities/page").handler(this::handleDummy);

        router.route("/api/v1/admin").handler(this::handleDummy);
        router.route("/api/v1/profile").handler(this::handleDummy);
        router.route("/api/v1/demo").handler(this::handleDummy);

        router.route("/api/v1/downloads").handler(this::handleDummy);
        router.post("/api/v1/activities/page").handler(this::handleDummy);

        for (RestApiRouter r : routers)
            r.install(router);
    }

    /**
     * @param shortName Class short name.
     * @return Full class name.
     */
    protected String igniteVisor(String shortName) {
        return VISOR_IGNITE + shortName;
    }

    /**
     * @param taskId Task ID.
     * @param taskCls Task class name.
     * @param argCls Arguments classes names.
     */
    protected void registerVisorTask(String taskId, String taskCls, String... argCls) {
        visorTasks.put(taskId, new VisorTaskDescriptor(taskCls, argCls));
    }

    /**
     * Register Visor tasks.
     */
    protected void registerVisorTasks() {
        registerVisorTask("querySql", igniteVisor("query.VisorQueryTask"), igniteVisor("query.VisorQueryArg"));
        registerVisorTask("querySqlV2", igniteVisor("query.VisorQueryTask"), igniteVisor("query.VisorQueryArgV2"));
        registerVisorTask("querySqlV3", igniteVisor("query.VisorQueryTask"), igniteVisor("query.VisorQueryArgV3"));
        registerVisorTask("querySqlX2", igniteVisor("query.VisorQueryTask"), igniteVisor("query.VisorQueryTaskArg"));

        registerVisorTask("queryScanX2", igniteVisor("query.VisorScanQueryTask"), igniteVisor("query.VisorScanQueryTaskArg"));

        registerVisorTask("queryFetch", igniteVisor("query.VisorQueryNextPageTask"), "org.apache.ignite.lang.IgniteBiTuple", "java.lang.String", "java.lang.Integer");
        registerVisorTask("queryFetchX2", igniteVisor("query.VisorQueryNextPageTask"), igniteVisor("query.VisorQueryNextPageTaskArg"));

        registerVisorTask("queryFetchFirstPage", igniteVisor("query.VisorQueryFetchFirstPageTask"), igniteVisor("query.VisorQueryNextPageTaskArg"));

        registerVisorTask("queryClose", igniteVisor("query.VisorQueryCleanupTask"), "java.util.Map", "java.util.UUID", "java.util.Set");
        registerVisorTask("queryCloseX2", igniteVisor("query.VisorQueryCleanupTask"), igniteVisor("query.VisorQueryCleanupTaskArg"));

        registerVisorTask("toggleClusterState", igniteVisor("misc.VisorChangeGridActiveStateTask"), igniteVisor("misc.VisorChangeGridActiveStateTaskArg"));

        registerVisorTask("cacheNamesCollectorTask", igniteVisor("cache.VisorCacheNamesCollectorTask"), "java.lang.Void");

        registerVisorTask("cacheNodesTask", igniteVisor("cache.VisorCacheNodesTask"), "java.lang.String");
        registerVisorTask("cacheNodesTaskX2", igniteVisor("cache.VisorCacheNodesTask"), igniteVisor("cache.VisorCacheNodesTaskArg"));
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
     * @param ctx Context.
     * @param data Data to send.
     */
    private void sendResult(RoutingContext ctx, Buffer data) {
        ctx
            .response()
            .putHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
            .putHeader(HttpHeaderNames.CACHE_CONTROL, HTTP_CACHE_CONTROL)
            .putHeader(HttpHeaderNames.PRAGMA, HttpHeaderValues.NO_CACHE)
            .putHeader(HttpHeaderNames.EXPIRES, "0")
            .setStatusCode(HTTP_OK)
            .end(data);
    }

    /**
     * @param ctx Context.
     * @param data Data to send.
     */
    private void sendResult(RoutingContext ctx, JsonObject data) {
        sendResult(ctx, data.toBuffer());
    }

    /**
     * @param ctx Context.
     * @param msg Error message to send.
     * @param e Error to send.
     */
    private void sendError(RoutingContext ctx, String msg, Throwable e) {
        ignite.log().error(msg, e);

        sendStatus(ctx, HTTP_INTERNAL_ERROR, msg + ": " + errorMessage(e));
    }

    /**
     * @param ctx Context
     */
    private void handleDummy(RoutingContext ctx) {
        ignite.log().info("Dummy: " + ctx.request().path());

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

            oldTop = newTop;
        }

        JsonObject json = new JsonObject()
            .put("count", clusters.size())
            .put("hasDemo", false)
            .put("clusters", new JsonArray().add(oldTop)); // TODO IGNITE-5617 quick hack for prototype.

        vertx.eventBus().send(Addresses.AGENTS_STATUS, json);
    }

    /**
     * TODO IGNITE-5617
     * @param desc Task descriptor.
     * @param nids Node IDs.
     * @param args Task arguments.
     * @return JSON object with VisorGatewayTask REST descriptor.
     */
    protected JsonObject prepareNodeVisorParams(VisorTaskDescriptor desc, String nids, JsonArray args) {
        JsonObject exeParams =  new JsonObject()
            .put("cmd", "exe")
            .put("name", "org.apache.ignite.internal.visor.compute.VisorGatewayTask")
            .put("p1", nids)
            .put("p2", desc.getTaskClass());

            AtomicInteger idx = new AtomicInteger(3);

            Arrays.stream(desc.getArgumentsClasses()).forEach(arg ->  exeParams.put("p" + idx.getAndIncrement(), arg));

            args.forEach(arg -> exeParams.put("p" + idx.getAndIncrement(), arg));

            return exeParams;
    }

    /**
     * @param be Bridge event
     */
    protected void handleNodeVisorMessages(BridgeEvent be) {
        if (be.type() == BridgeEventType.SEND) {
            JsonObject msg = be.getRawMessage();

            if (msg != null) {
                String addr = msg.getString("address");

                if ("node:visor".equals(addr)) {
                    JsonObject body = msg.getJsonObject("body");

                    if (body != null) {
                        JsonObject params = body.getJsonObject("params");

                        String taskId = params.getString("taskId");

                        if (!F.isEmpty(taskId)) {
                            VisorTaskDescriptor desc = visorTasks.get(taskId);

                            JsonObject exeParams = prepareNodeVisorParams(desc, params.getString("nids"), params.getJsonArray("args"));

                            body.put("params", exeParams);

                            msg.put("body", body);
                        }
                    }
                }
            }
        }

        be.complete(true);
    }

    /**
     * Visor task descriptor.
     */
    public static class VisorTaskDescriptor {
        /** */
        private static final String[] EMPTY = new String[0];

        /** */
        private final String taskCls;

        /** */
        private final String[] argCls;

        /**
         * @param taskCls Visor task class.
         * @param argCls Visor task arguments classes.
         */
        private VisorTaskDescriptor(String taskCls, String[] argCls) {
            this.taskCls = taskCls;
            this.argCls = argCls != null ? argCls : EMPTY;
        }

        /**
         * @return Visor task class.
         */
        public String getTaskClass() {
            return taskCls;
        }

        /**
         * @return Visor task arguments classes.
         */
        public String[] getArgumentsClasses() {
            return argCls;
        }
    }
}

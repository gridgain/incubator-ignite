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

import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.bridge.BridgeEventType;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CookieHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.handler.UserSessionHandler;
import io.vertx.ext.web.handler.sockjs.BridgeEvent;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.sstore.LocalSessionStore;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.console.auth.IgniteAuth;
import org.apache.ignite.console.common.Addresses;
import org.apache.ignite.console.routes.ConfigurationsRouter;
import org.apache.ignite.console.routes.NotebooksRouter;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.client.GridClientCacheMode;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlIndexMetadata;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlMetadata;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.client.message.GridClientCacheBean;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeBean;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeMetricsBean;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.visor.compute.VisorGatewayTask;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.apache.ignite.console.common.Utils.errorMessage;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BINARY_CONFIGURATION;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CACHE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_NODE_CONSISTENT_ID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_TCP_ADDRS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_TCP_HOST_NAMES;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_TCP_PORT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_SUBJECT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_TX_CONFIG;
import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper.IGNITE_BINARY_OBJECT_SERIALIZER;
import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper.IGNITE_SQL_INDEX_METADATA_SERIALIZER;
import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper.IGNITE_SQL_METADATA_SERIALIZER;
import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper.IGNITE_TUPLE_SERIALIZER;
import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper.IGNITE_UUID_SERIALIZER;
import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper.THROWABLE_SERIALIZER;

/**
 * Web Console server.
 */
public class WebConsoleServer extends AbstractVerticle {
    /** */
    private static final String VISOR_IGNITE = "org.apache.ignite.internal.visor.";

    /** */
    private static final String CLUSTER_ID = UUID.randomUUID().toString();

    /** */
    private static final List<CharSequence> HTTP_CACHE_CONTROL = Arrays.asList(
        HttpHeaderValues.NO_CACHE,
        HttpHeaderValues.NO_STORE,
        HttpHeaderValues.MUST_REVALIDATE);

    /** */
    private final Map<String, VisorTaskDescriptor> visorTasks = new ConcurrentHashMap<>();

    /** */
    private final Map<String, JsonObject> clusters = new ConcurrentHashMap<>();

    /** */
    private final Ignite ignite;

    /** */
    private final IgniteAuth auth;

    /** */
    private final ConfigurationsRouter configurationsRouter;

    /** */
    private final NotebooksRouter notebooksRouter;

    /** */
    private boolean embedded;

    // TODO IGNITE-5617 quick hack for prototype.
    static {
        SimpleModule module = new SimpleModule();

        module.addSerializer(Throwable.class, THROWABLE_SERIALIZER);
        module.addSerializer(IgniteBiTuple.class, IGNITE_TUPLE_SERIALIZER);
        module.addSerializer(IgniteUuid.class, IGNITE_UUID_SERIALIZER);
        module.addSerializer(GridCacheSqlMetadata.class, IGNITE_SQL_METADATA_SERIALIZER);
        module.addSerializer(GridCacheSqlIndexMetadata.class, IGNITE_SQL_INDEX_METADATA_SERIALIZER);
        module.addSerializer(BinaryObjectImpl.class, IGNITE_BINARY_OBJECT_SERIALIZER);

        Json.mapper.registerModule(module);
    }

    /**
     * @param s Message to log.
     */
    protected void logInfo(Object s) {
        ignite.log().info(String.valueOf(s));
    }

    /**
     * @param msg Message to log.
     * @param e Error to log.
     */
    protected void logError(String msg, Throwable e) {
        ignite.log().error(msg, e);
    }

    /**
     * @param ignite Ignite.
     * @param auth Auth provider.
     * @param embedded Whether Web Console run in embedded mode.
     */
    public WebConsoleServer(Ignite ignite, IgniteAuth auth, boolean embedded) {
        this.ignite = ignite;
        this.auth = auth;
        this.embedded = embedded;

        configurationsRouter = new ConfigurationsRouter(ignite);
        notebooksRouter = new NotebooksRouter(ignite);
    }

    /** {@inheritDoc} */
    @Override public void start() {
        long start = System.currentTimeMillis();

        logInfo("Embedded mode: " + embedded);

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
        router.route().handler(SessionHandler.create(LocalSessionStore.create(vertx)));
        router.route().handler(UserSessionHandler.create(auth));

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

        logInfo("Web Console started: " + (System.currentTimeMillis() - start));
    }

    /**
     * Register event bus consumers.
     */
    private void registerEventBusConsumers() {
        EventBus evtBus = vertx.eventBus();

        if (embedded) {
            evtBus.consumer(Addresses.NODE_REST, this::handleEmbeddedNodeRest);
            evtBus.consumer(Addresses.NODE_VISOR, this::handleEmbeddedNodeVisor);

            vertx.setPeriodic(3000, this::handleEmbeddedClusterTopology);
        }
        else {
            evtBus.consumer(Addresses.CLUSTER_TOPOLOGY, this::handleClusterTopology);

            vertx.setPeriodic(3000, this::refreshTop);
        }
    }

    /**
     * @param tid Timer ID.
     */
    private void refreshTop(long tid) {
        JsonObject json = new JsonObject()
            .put("count", 1)
            .put("hasDemo", false);

        JsonArray zz = new JsonArray(); // TODO IGNITE-5617 temporary hack

        clusters.forEach((k, v) -> {
            zz.add(v);
        });

        json.put("clusters", zz);

        vertx.eventBus().send(Addresses.AGENTS_STATUS, json);
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

        router.route("/api/v1/downloads").handler(this::handleDummy);
        router.post("/api/v1/activities/page").handler(this::handleDummy);

        configurationsRouter.install(router);
        notebooksRouter.install(router);
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
        logError(msg, e);

        sendStatus(ctx, HTTP_INTERNAL_ERROR, msg + ": " + errorMessage(e));
    }

    /**
     * Get the authenticated user (if any).
     * If user not found, send {@link HttpURLConnection#HTTP_UNAUTHORIZED}.
     *
     * @param ctx Context
     * @return User or {@code null} if the current user is not authenticated.
     */
    @Nullable private User checkUser(RoutingContext ctx) {
        User user = ctx.user();

        if (user == null)
            sendStatus(ctx, HTTP_UNAUTHORIZED);

        return user;
    }

    /**
     * @param ctx Context
     */
    private void handleUser(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null)
            sendResult(ctx, user.principal());
    }

    /**
     * @param ctx Context
     */
    private void handleSignUp(RoutingContext ctx) {
        JsonObject authInfo = ctx.getBodyAsJson();

        authInfo.put("signup", true);

        auth.authenticate(authInfo, asyncRes -> {
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
    private void handleDummy(RoutingContext ctx) {
        logInfo("Dummy: " + ctx.request().path());

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
     * @param tid Timer ID.
     */
    private void handleEmbeddedClusterTopology(long tid) {
        try {
            JsonObject desc = clusters.computeIfAbsent(CLUSTER_ID, key -> {
                JsonArray top = new JsonArray();

                top.add(new JsonObject()
                    .put("id", CLUSTER_ID)
                    .put("name", "EMBEDDED")
                    .put("nids", new JsonArray().add(ignite.cluster().localNode().id().toString()))
                    .put("addresses", new JsonArray().add("192.168.0.10"))
                    .put("clusterVersion", ignite.version().toString())
                    .put("active", ignite.cluster().active())
                    .put("secured", false)
                );

                return new JsonObject()
                    .put("count", 1) // TODO IGNITE-5617 temporary hack
                    .put("hasDemo", false)
                    .put("clusters", top);
            });

            // TODO IGNITE-5617  Update cluster.active if ignite.cluster().active() state changed!

            vertx.eventBus().send(Addresses.AGENTS_STATUS, desc);
        }
        catch (Throwable ignore) {
            LT.info(ignite.log(), "Embedded Web Console awaits for cluster activation...");
        }
    }

    /**
     * Handle REST commands.
     *
     * @param msg Message with data.
     */
    private void handleEmbeddedNodeRest(Message<JsonObject> msg) {
        JsonObject params = msg.body().getJsonObject("params"); // TODO IGNITE-5617 check for NPE?

        String cmd = params.getString("cmd");

        if ("top".equals(cmd)) {
            boolean attr = params.getBoolean("attr", false);
            boolean mtr = params.getBoolean("mtr", false);
            boolean caches = params.getBoolean("caches", false);

            GridKernalContext ctx = ((IgniteEx)ignite).context();

            Collection<ClusterNode> allNodes = F.concat(false,
                ctx.discovery().allNodes(), ctx.discovery().daemonNodes());

            Collection<GridClientNodeBean> top =
                new ArrayList<>(allNodes.size());

            for (ClusterNode node : allNodes)
                top.add(createNodeBean(ctx, node, mtr, attr, caches));

            GridRestResponse res = new GridRestResponse(top);

            JsonObject restRes = new JsonObject()
                .put("status", 0)
                .putNull("error")
                .put("data", Json.encode(res.getResponse()))
                .put("sessionToken", 1)
                .put("zipped", false);

            msg.reply(restRes);
        }
        else
            msg.fail(HTTP_BAD_REQUEST, "Unsupported REST command: " + params);
    }

    // TODO IGNITE-5617 quick hack for prototype.
    /**
     * Get node attribute by specified attribute name.
     *
     * @param node Node to get attribute for.
     * @param attrName Attribute name.
     * @param dfltVal Default result for case when node attribute resolved into {@code null}.
     * @return Attribute value or default result if requested attribute resolved into {@code null}.
     */
    private <T> T attribute(ClusterNode node, String attrName, T dfltVal) {
        T attr = node.attribute(attrName);

        return attr == null ? dfltVal : attr;
    }

    // TODO IGNITE-5617 quick hack for prototype.
    /**
     * @param col Collection;
     * @return Non-empty list.
     */
    private static Collection<String> nonEmptyList(Collection<String> col) {
        return col == null ? Collections.emptyList() : col;
    }

    // TODO IGNITE-5617 quick hack for prototype.
    /**
     * Creates cache bean.
     *
     * @param ccfg Cache configuration.
     * @return Cache bean.
     */
    public GridClientCacheBean createCacheBean(CacheConfiguration ccfg) {
        GridClientCacheBean cacheBean = new GridClientCacheBean();

        cacheBean.setName(ccfg.getName());
        cacheBean.setMode(GridClientCacheMode.valueOf(ccfg.getCacheMode().toString()));
        cacheBean.setSqlSchema(ccfg.getSqlSchema());

        return cacheBean;
    }

    // TODO IGNITE-5617 quick hack for prototype.
    /**
     * Creates node bean out of cluster node. Notice that cache attribute is handled separately.
     *
     * @param node Cluster node.
     * @param mtr Whether to include node metrics.
     * @param attr Whether to include node attributes.
     * @param caches Whether to include node caches.
     * @return Grid Node bean.
     */
    private GridClientNodeBean createNodeBean(GridKernalContext ctx, ClusterNode node, boolean mtr, boolean attr, boolean caches) {
        assert node != null;

        GridClientNodeBean nodeBean = new GridClientNodeBean();

        nodeBean.setNodeId(node.id());
        nodeBean.setConsistentId(node.consistentId());
        nodeBean.setTcpPort(attribute(node, ATTR_REST_TCP_PORT, 0));
        nodeBean.setOrder(node.order());

        nodeBean.setTcpAddresses(nonEmptyList(node.attribute(ATTR_REST_TCP_ADDRS)));
        nodeBean.setTcpHostNames(nonEmptyList(node.attribute(ATTR_REST_TCP_HOST_NAMES)));

        if (caches) {
            Map<String, CacheConfiguration> nodeCaches = ctx.discovery().nodePublicCaches(node);

            Collection<GridClientCacheBean> cacheBeans = new ArrayList<>(nodeCaches.size());

            for (CacheConfiguration ccfg : nodeCaches.values())
                cacheBeans.add(createCacheBean(ccfg));

            nodeBean.setCaches(cacheBeans);
        }

        if (mtr) {
            ClusterMetrics metrics = node.metrics();

            GridClientNodeMetricsBean metricsBean = new GridClientNodeMetricsBean();

            metricsBean.setStartTime(metrics.getStartTime());
            metricsBean.setAverageActiveJobs(metrics.getAverageActiveJobs());
            metricsBean.setAverageCancelledJobs(metrics.getAverageCancelledJobs());
            metricsBean.setAverageCpuLoad(metrics.getAverageCpuLoad());
            metricsBean.setAverageJobExecuteTime(metrics.getAverageJobExecuteTime());
            metricsBean.setAverageJobWaitTime(metrics.getAverageJobWaitTime());
            metricsBean.setAverageRejectedJobs(metrics.getAverageRejectedJobs());
            metricsBean.setAverageWaitingJobs(metrics.getAverageWaitingJobs());
            metricsBean.setCurrentActiveJobs(metrics.getCurrentActiveJobs());
            metricsBean.setCurrentCancelledJobs(metrics.getCurrentCancelledJobs());
            metricsBean.setCurrentCpuLoad(metrics.getCurrentCpuLoad());
            metricsBean.setCurrentGcCpuLoad(metrics.getCurrentGcCpuLoad());
            metricsBean.setCurrentDaemonThreadCount(metrics.getCurrentDaemonThreadCount());
            metricsBean.setCurrentIdleTime(metrics.getCurrentIdleTime());
            metricsBean.setCurrentJobExecuteTime(metrics.getCurrentJobExecuteTime());
            metricsBean.setCurrentJobWaitTime(metrics.getCurrentJobWaitTime());
            metricsBean.setCurrentRejectedJobs(metrics.getCurrentRejectedJobs());
            metricsBean.setCurrentThreadCount(metrics.getCurrentThreadCount());
            metricsBean.setCurrentWaitingJobs(metrics.getCurrentWaitingJobs());
            metricsBean.setHeapMemoryCommitted(metrics.getHeapMemoryCommitted());
            metricsBean.setHeapMemoryInitialized(metrics.getHeapMemoryInitialized());
            metricsBean.setHeapMemoryMaximum(metrics.getHeapMemoryMaximum());
            metricsBean.setHeapMemoryUsed(metrics.getHeapMemoryUsed());
            metricsBean.setLastDataVersion(metrics.getLastDataVersion());
            metricsBean.setLastUpdateTime(metrics.getLastUpdateTime());
            metricsBean.setMaximumActiveJobs(metrics.getMaximumActiveJobs());
            metricsBean.setMaximumCancelledJobs(metrics.getMaximumCancelledJobs());
            metricsBean.setMaximumJobExecuteTime(metrics.getMaximumJobExecuteTime());
            metricsBean.setMaximumJobWaitTime(metrics.getMaximumJobWaitTime());
            metricsBean.setMaximumRejectedJobs(metrics.getMaximumRejectedJobs());
            metricsBean.setMaximumThreadCount(metrics.getMaximumThreadCount());
            metricsBean.setMaximumWaitingJobs(metrics.getMaximumWaitingJobs());
            metricsBean.setNodeStartTime(metrics.getNodeStartTime());
            metricsBean.setNonHeapMemoryCommitted(metrics.getNonHeapMemoryCommitted());
            metricsBean.setNonHeapMemoryInitialized(metrics.getNonHeapMemoryInitialized());
            metricsBean.setNonHeapMemoryMaximum(metrics.getNonHeapMemoryMaximum());
            metricsBean.setNonHeapMemoryUsed(metrics.getNonHeapMemoryUsed());
            metricsBean.setStartTime(metrics.getStartTime());
            metricsBean.setTotalCancelledJobs(metrics.getTotalCancelledJobs());
            metricsBean.setTotalCpus(metrics.getTotalCpus());
            metricsBean.setTotalExecutedJobs(metrics.getTotalExecutedJobs());
            metricsBean.setTotalIdleTime(metrics.getTotalIdleTime());
            metricsBean.setTotalRejectedJobs(metrics.getTotalRejectedJobs());
            metricsBean.setTotalStartedThreadCount(metrics.getTotalStartedThreadCount());
            metricsBean.setTotalExecutedTasks(metrics.getTotalExecutedTasks());
            metricsBean.setSentMessagesCount(metrics.getSentMessagesCount());
            metricsBean.setSentBytesCount(metrics.getSentBytesCount());
            metricsBean.setReceivedMessagesCount(metrics.getReceivedMessagesCount());
            metricsBean.setReceivedBytesCount(metrics.getReceivedBytesCount());
            metricsBean.setUpTime(metrics.getUpTime());

            nodeBean.setMetrics(metricsBean);
        }

        if (attr) {
            Map<String, Object> attrs = new HashMap<>(node.attributes());

            attrs.remove(ATTR_CACHE);
            attrs.remove(ATTR_TX_CONFIG);
            attrs.remove(ATTR_SECURITY_SUBJECT);
            attrs.remove(ATTR_SECURITY_SUBJECT_V2);
            attrs.remove(ATTR_SECURITY_CREDENTIALS);
            attrs.remove(ATTR_BINARY_CONFIGURATION);
            attrs.remove(ATTR_NODE_CONSISTENT_ID);

            for (Iterator<Map.Entry<String, Object>> i = attrs.entrySet().iterator(); i.hasNext();) {
                Map.Entry<String, Object> e = i.next();

                if (!e.getKey().startsWith("org.apache.ignite.") && !e.getKey().startsWith("plugins.") &&
                    System.getProperty(e.getKey()) == null) {
                    i.remove();

                    continue;
                }

                if (e.getValue() != null) {
                    if (e.getValue().getClass().isEnum() || e.getValue() instanceof InetAddress)
                        e.setValue(e.getValue().toString());
                    else if (e.getValue().getClass().isArray())
                        i.remove();
                }
            }

            nodeBean.setAttributes(attrs);
        }

        return nodeBean;
    }


    /**
     * Handle Visor commands.
     *
     * @param msg Message with data.
     */
    private void handleEmbeddedNodeVisor(Message<JsonObject> msg) {
        try {
            JsonObject params = msg.body().getJsonObject("params"); // TODO IGNITE-5617 check for NPE?

            String taskId = params.getString("taskId");

            VisorTaskDescriptor desc = visorTasks.get(taskId);

            JsonArray z = params.getJsonArray("args");

            int sz = z.size() + desc.getArgumentsClasses().length + 2;

            JsonObject execParams = prepareNodeVisorParams(desc, params.getString("nids"), z);

            List<Object> args = new ArrayList<>();

            for (int i = 0; i < sz; i++)
                args.add(String.valueOf(execParams.getValue("p" + (i + 1))));

            IgniteCompute compute = ignite.compute();

            Object res = compute.execute(VisorGatewayTask.class, args.toArray());

            JsonObject restRes = new JsonObject()
                .put("status", 0)
                .putNull("error")
                .put("data", Json.encode(res))
                .put("sessionToken", 1)
                .put("zipped", false);

            msg.reply(restRes);
        }
        catch (Throwable e) {
            msg.fail(HTTP_INTERNAL_ERROR, errorMessage(e));
        }
    }

    /**
     * TODO IGNITE-5617
     * @param desc Task descriptor.
     * @param nids Node IDs.
     * @param args Task arguments.
     * @return JSON object with VisorGatewayTask REST descriptor.
     */
    private JsonObject prepareNodeVisorParams(VisorTaskDescriptor desc, String nids, JsonArray args) {
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
    private void handleNodeVisorMessages(BridgeEvent be) {
        if (!embedded && be.type() == BridgeEventType.SEND) {
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
    private static class VisorTaskDescriptor {
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

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

package org.apache.ignite.console.verticles;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.cache.Cache;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AbstractVerticle;
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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.console.auth.IgniteAuth;
import org.apache.ignite.console.common.Addresses;
import org.apache.ignite.console.common.Consts;
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
import org.apache.ignite.internal.visor.compute.VisorGatewayTask;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
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
@SuppressWarnings("JavaAbbreviationUsage")
public class WebConsoleVerticle extends AbstractVerticle {
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
    private static final String CLUSTER_ID = UUID.randomUUID().toString();

    /** */
    private static final List<CharSequence> CACHE_CONTROL = Arrays.asList(
        HttpHeaderValues.NO_CACHE,
        HttpHeaderValues.NO_STORE,
        HttpHeaderValues.MUST_REVALIDATE);

    /** */
    private Map<String, VisorTaskDescriptor> visorTasks = new ConcurrentHashMap<>();

    /** */
    private Map<String, JsonObject> clusters = new ConcurrentHashMap<>();

    /** */
    private final Ignite ignite;

    /** */
    private final IgniteAuth auth;

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
    private static void log(Object s) {
        System.out.println("[" + DEBUG_DATE_FMT.format(new Date(System.currentTimeMillis())) + "] [" +
            Thread.currentThread().getName() + "]" + " " + s);
    }

    /**
     * @param ignite Ignite.
     * @param auth Auth provider.
     * @param embedded Whether Web Console run in embedded mode.
     */
    public WebConsoleVerticle(Ignite ignite, IgniteAuth auth, boolean embedded) {
        this.ignite = ignite;
        this.auth = auth;
        this.embedded = embedded;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        long start = System.currentTimeMillis();

        log("Embedded mode: " + embedded);

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

        log("Web Console started: " + (System.currentTimeMillis() - start));
    }

    /** // TODO IGNITE-5617  Javadocs */
    private void registerEventBusConsumers() {
        EventBus eventBus = vertx.eventBus();

        if (embedded) {
            eventBus.consumer(Addresses.NODE_REST, this::handleEmbeddedNodeRest);
            eventBus.consumer(Addresses.NODE_VISOR, this::handleEmbeddedNodeVisor);

            // TODO IGNITE-5617 quick hack for prototype.
            JsonArray top = new JsonArray();
             top.add(new JsonObject()
                .put("id", CLUSTER_ID)
                .put("name", "EMBBEDED")
                .put("nids", new JsonArray().add(ignite.cluster().localNode().id().toString()))
                .put("addresses", new JsonArray().add("192.168.0.10"))
                .put("clusterVersion", ignite.version().toString())
                .put("active", ignite.cluster().active())
                .put("secured", false)
             );

            JsonObject desc = new JsonObject()
                .put("count", 1)
                .put("hasDemo", false)
                .put("clusters", top);

            clusters.put(CLUSTER_ID, desc);

            vertx.setPeriodic(3000, tid -> eventBus.send(Addresses.AGENTS_STATUS, desc)); // TODO IGNITE-5617 quick hack for prototype.
        }
        else
            eventBus.consumer(Addresses.CLUSTER_TOPOLOGY, this::handleClusterTopology);
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
     * @param taskId Task ID.
     * @param taskCls Task class name.
     * @param argCls Arguments classes names.
     */
    private void registerVisorTask(String taskId, String taskCls, String... argCls) {
        visorTasks.put(taskId, new VisorTaskDescriptor(taskCls, argCls));
    }

    /**
     * Register Visor tasks.
     */
    private void registerVisorTasks() {
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

        // ---------- GG ---------------

        registerVisorTask("collectCacheRebalance",
            igniteVisor("node.VisorCacheRebalanceCollectorTask"),
            igniteVisor("node.VisorCacheRebalanceCollectorTaskArg"));

        registerVisorTask("collectorTask", igniteVisor("node.VisorNodeDataCollectorTask"),
            igniteVisor("node.VisorNodeDataCollectorTaskArg"));
        registerVisorTask("ggCollectorTask", gridgainVisor("node.VisorGridGainNodeDataCollectorTask"),
            igniteVisor("node.VisorNodeDataCollectorTaskArg"));

        registerVisorTask("queryRunning", igniteVisor("query.VisorCollectRunningQueriesTask"), "java.lang.Long");
        registerVisorTask("queryRunningX2", igniteVisor("query.VisorRunningQueriesCollectorTask"), igniteVisor("query.VisorRunningQueriesCollectorTaskArg"));

        registerVisorTask("queryCancel", igniteVisor("query.VisorCancelQueriesTask"), "java.util.Collection", "java.lang.Long");
        registerVisorTask("queryCancelX2", igniteVisor("query.VisorQueryCancelTask"), igniteVisor("query.VisorQueryCancelTaskArg"));

        registerVisorTask("sessionCancel", igniteVisor("compute.VisorComputeCancelSessionsTask"),
            igniteVisor("compute.VisorComputeCancelSessionsTaskArg"), "java.util.Set", "org.apache.ignite.lang.IgniteUuid");

        registerVisorTask("toggleTaskMonitoring", igniteVisor("compute.VisorComputeToggleMonitoringTask"), "org.apache.ignite.lang.IgniteBiTuple", "java.lang.String", "java.lang.Boolean");
        registerVisorTask("toggleTaskMonitoringX2", igniteVisor("compute.VisorComputeToggleMonitoringTask"), igniteVisor("compute.VisorComputeToggleMonitoringTaskArg"));

        registerVisorTask("queryDetailMetrics", igniteVisor("cache.VisorCacheQueryDetailMetricsCollectorTask"), "java.lang.Long");
        registerVisorTask("queryDetailMetricsX2", igniteVisor("query.VisorQueryDetailMetricsCollectorTask"), igniteVisor("query.VisorQueryDetailMetricsCollectorTaskArg"));

        registerVisorTask("queryResetDetailMetrics", igniteVisor("cache.VisorCacheResetQueryDetailMetricsTask"), "java.lang.Void");
        registerVisorTask("queryResetDetailMetricsX2", igniteVisor("query.VisorQueryResetDetailMetricsTask"), "java.lang.Void");

        registerVisorTask("services", igniteVisor("service.VisorServiceTask"), "java.lang.Void");

        registerVisorTask("serviceCancel", igniteVisor("service.VisorCancelServiceTask"), "java.lang.String");
        registerVisorTask("serviceCancelX2", igniteVisor("service.VisorCancelServiceTask"), igniteVisor("service.VisorCancelServiceTaskArg"));

        registerVisorTask("nodeConfiguration", igniteVisor("node.VisorNodeConfigurationCollectorTask"), "java.lang.Void");
        registerVisorTask("ggNodeConfiguration", gridgainVisor("node.VisorGridGainNodeConfigurationCollectorTask"), "java.lang.Void");

        registerVisorTask("nodeGc", igniteVisor("node.VisorNodeGcTask"), "java.lang.Void");

        registerVisorTask("nodePing", igniteVisor("node.VisorNodePingTask"), "java.util.UUID");
        registerVisorTask("nodePingX2", igniteVisor("node.VisorNodePingTask"), igniteVisor("node.VisorNodePingTaskArg"));

        registerVisorTask("nodeThreadDump", igniteVisor("debug.VisorThreadDumpTask"), "java.lang.Void");

        registerVisorTask("nodeStop", igniteVisor("node.VisorNodeStopTask"), "java.lang.Void");

        registerVisorTask("nodeRestart", igniteVisor("node.VisorNodeRestartTask"), "java.lang.Void");

        registerVisorTask("cacheConfiguration", igniteVisor("cache.VisorCacheConfigurationCollectorTask"), "java.util.Collection", "org.apache.ignite.lang.IgniteUuid");
        registerVisorTask("cacheConfigurationX2", igniteVisor("cache.VisorCacheConfigurationCollectorTask"), igniteVisor("cache.VisorCacheConfigurationCollectorTaskArg"));

        registerVisorTask("ggCacheConfiguration", gridgainVisor("cache.VisorGridGainCacheConfigurationCollectorTask"), "java.util.Collection", "org.apache.ignite.lang.IgniteUuid");
        registerVisorTask("ggCacheConfigurationX2", gridgainVisor("cache.VisorGridGainCacheConfigurationCollectorTask"), igniteVisor("cache.VisorCacheConfigurationCollectorTaskArg"));

        registerVisorTask("cacheStart", igniteVisor("cache.VisorCacheStartTask"), igniteVisor("cache.VisorCacheStartTask$VisorCacheStartArg"));
        registerVisorTask("cacheStartX2", igniteVisor("cache.VisorCacheStartTask"), igniteVisor("cache.VisorCacheStartTaskArg"));

        registerVisorTask("cacheClear", igniteVisor("cache.VisorCacheClearTask"), "java.lang.String");
        registerVisorTask("cacheClearX2", igniteVisor("cache.VisorCacheClearTask"), igniteVisor("cache.VisorCacheClearTaskArg"));

        registerVisorTask("cacheStop", igniteVisor("cache.VisorCacheStopTask"), "java.lang.String");
        registerVisorTask("cacheStopX2", igniteVisor("cache.VisorCacheStopTask"), igniteVisor("cache.VisorCacheStopTaskArg"));
        registerVisorTask("cacheStopX3", igniteVisor("cache.VisorCacheStopTask"), igniteVisor("cache.VisorCacheStopTaskArg"), "java.util.List", "java.lang.String");

        registerVisorTask("cachePartitions", igniteVisor("cache.VisorCachePartitionsTask"), "java.lang.String");
        registerVisorTask("cachePartitionsX2", igniteVisor("cache.VisorCachePartitionsTask"), igniteVisor("cache.VisorCachePartitionsTaskArg"));

        registerVisorTask("cacheResetMetrics", igniteVisor("cache.VisorCacheResetMetricsTask"), "java.lang.String");
        registerVisorTask("cacheResetMetricsX2", igniteVisor("cache.VisorCacheResetMetricsTask"), igniteVisor("cache.VisorCacheResetMetricsTaskArg"));

        registerVisorTask("cacheRebalance", igniteVisor("cache.VisorCacheRebalanceTask"), "java.util.Set", "java.lang.String");
        registerVisorTask("cacheRebalanceX2", igniteVisor("cache.VisorCacheRebalanceTask"), igniteVisor("cache.VisorCacheRebalanceTaskArg"), "java.util.Set", "java.lang.String");

        registerVisorTask("cacheToggleStatistics", igniteVisor("cache.VisorCacheToggleStatisticsTask"), igniteVisor("cache.VisorCacheToggleStatisticsTaskArg"));

        registerVisorTask("updateLicense", gridgainVisor("license.VisorLicenseUpdateTask"), "org.apache.ignite.lang.IgniteBiTuple", "java.util.UUID", "java.lang.String");
        registerVisorTask("updateLicenseX2", gridgainVisor("license.VisorLicenseUpdateTask"), gridgainVisor("license.VisorLicenseUpdateTaskArg"));

        registerVisorTask("changeClusterActiveState", igniteVisor("misc.VisorChangeGridActiveStateTask"), igniteVisor("misc.VisorChangeGridActiveStateTaskArg"));

        registerVisorTask("checkSnapshots", snapshotsVisor("VisorCheckSnapshotsChangesTask"), "java.lang.Void");
        registerVisorTask("collectSnapshots", snapshotsVisor("VisorListSnapshotsTask"), snapshotsVisor("VisorSnapshotInfo"));
        registerVisorTask("snapshotCreate", snapshotsVisor("VisorCreateSnapshotTask"), snapshotsVisor("VisorSnapshotInfo"));
        registerVisorTask("snapshotRestore", snapshotsVisor("VisorRestoreSnapshotTask"), snapshotsVisor("VisorSnapshotInfo"));
        registerVisorTask("snapshotDelete", snapshotsVisor("VisorDeleteSnapshotTask"), snapshotsVisor("VisorSnapshotInfo"));
        registerVisorTask("snapshotMove", snapshotsVisor("VisorMoveSnapshotTask"), snapshotsVisor("VisorSnapshotInfo"));
        registerVisorTask("snapshotsStatus", snapshotsVisor("VisorSnapshotsStatusTask"), "java.lang.Void");
        registerVisorTask("snapshotCancel", snapshotsVisor("VisorCancelSnapshotOperationTask"), snapshotsVisor("VisorCancelSnapshotOperationTaskArg"));

        registerVisorTask("collectSchedules", snapshotsVisor("VisorCollectSnapshotSchedulesTask"), "java.lang.Void");
        registerVisorTask("scheduleSnapshotOperation", snapshotsVisor("VisorScheduleSnapshotOperationTask"), snapshotsVisor("VisorSnapshotSchedule"));
        registerVisorTask("toggleSnapshotScheduleEnabledState", snapshotsVisor("VisorToggleSnapshotScheduleEnabledStateTask"), snapshotsVisor("VisorSnapshotSchedule"));
        registerVisorTask("deleteSnapshotSchedule", snapshotsVisor("VisorDeleteSnapshotScheduleTask"), snapshotsVisor("VisorSnapshotSchedule"));

        registerVisorTask("latestTextFiles", igniteVisor("file.VisorLatestTextFilesTask"), "org.apache.ignite.lang.IgniteBiTuple", "java.lang.String", "java.lang.String");
        registerVisorTask("latestTextFilesX2", igniteVisor("file.VisorLatestTextFilesTask"), igniteVisor("file.VisorLatestTextFilesTaskArg"));

        registerVisorTask("fileBlockTask", igniteVisor("file.VisorFileBlockTask"), igniteVisor("file.VisorFileBlockTask$VisorFileBlockArg"));
        registerVisorTask("fileBlockTaskX2", igniteVisor("file.VisorFileBlockTask"), igniteVisor("file.VisorFileBlockTaskArg"));

        registerVisorTask("baseline", igniteVisor("baseline.VisorBaselineTask"), igniteVisor("baseline.VisorBaselineTaskArg"));
        registerVisorTask("baselineView", igniteVisor("baseline.VisorBaselineViewTask"), "java.lang.Void");

        registerVisorTask("drResetMetrics", drVisor("VisorDrResetMetricsTask"), "java.lang.Void");
        registerVisorTask("drBootstrap", drVisor("VisorDrSenderCacheBootstrapTask"), drVisor("VisorDrSenderCacheBootstrapTaskArg"));
        registerVisorTask("drChangeReplicationState", drVisor("VisorDrSenderCacheChangeReplicationStateTask"), drVisor("VisorDrSenderCacheChangeReplicationStateTaskArg"));
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
     * @param status Status to send.
     * @param data Data to send.
     */
    private void sendResult(RoutingContext ctx, int status, String data) {
        ctx
            .response()
            .putHeader(HttpHeaderNames.CACHE_CONTROL, CACHE_CONTROL)
            .putHeader(HttpHeaderNames.PRAGMA, HttpHeaderValues.NO_CACHE)
            .putHeader(HttpHeaderNames.EXPIRES, "0")
            .setStatusCode(status)
            .end(data);
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
            .putHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
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
        IgniteCache<String, String> cache = ignite.cache(Consts.NOTEBOOKS_CACHE_NAME);

        List<Cache.Entry<String, String>> list = cache.query(new ScanQuery<String, String>()).getAll();

        String res = list.stream().map(Cache.Entry::getValue).collect(Collectors.joining(",", "[", "]"));

        sendResult(ctx, HTTP_OK, res);
    }

    /**
     * @param ctx Context
     */
    private void handleNotebookSave(RoutingContext ctx) {
        User user = ctx.user();

        if (user == null)
            sendStatus(ctx, HTTP_UNAUTHORIZED, "User not found");
        else {
            // JsonObject account = user.principal();

            JsonObject notebook = ctx.getBody().toJsonObject();

            IgniteCache<String, String> cache = ignite.cache(Consts.NOTEBOOKS_CACHE_NAME);

            String _id = notebook.getString("_id");

            if (F.isEmpty(_id)) {
                _id = UUID.randomUUID().toString();

                notebook.put("_id", _id);
                notebook.put("paragraphs", new JsonArray());
            }

            String json = notebook.encode();

            cache.put(_id, json);

            sendStatus(ctx, HTTP_OK, json);
        }
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

            oldTop = newTop;
        }

        JsonObject json = new JsonObject()
            .put("count", clusters.size())
            .put("hasDemo", false)
            .put("clusters", new JsonArray().add(oldTop)); // TODO IGNITE-5617 quick hack for prototype.

        vertx.eventBus().send(Addresses.AGENTS_STATUS, json);
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

            JsonObject restResult = new JsonObject()
                .put("status", 0)
                .putNull("error")
                .put("data", Json.encode(res.getResponse()))
                .put("sessionToken", 1)
                .put("zipped", false);

            msg.reply(restResult);
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

            JsonObject restResult = new JsonObject()
                .put("status", 0)
                .putNull("error")
                .put("data", Json.encode(res))
                .put("sessionToken", 1)
                .put("zipped", false);

            msg.reply(restResult);
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
                String address = msg.getString("address");

                if ("node:visor".equals(address)) {
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
        private final String taskCls;

        /** */
        private final String[] argCls;

        /**
         * @param taskCls Visor task class.
         * @param argCls Visor task arguments classes.
         */
        private VisorTaskDescriptor(String taskCls, String[] argCls) {
            this.taskCls = taskCls;
            this.argCls = argCls != null ? argCls : new String[0];
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

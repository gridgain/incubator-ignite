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
    private Map<String, VisorTaskDescriptor> visorTasks = new ConcurrentHashMap<>();

    /** */
    private Map<String, JsonObject> clusters = new ConcurrentHashMap<>();

    /**
     * @param s Message to log.
     */
    private static void log(Object s) {
        System.out.println("[" + DEBUG_DATE_FMT.format(new Date(System.currentTimeMillis())) + "] [" +
            Thread.currentThread().getName() + "]" + " " + s);
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

        CacheConfiguration accountsCfg = new CacheConfiguration("accounts");
        accountsCfg.setCacheMode(REPLICATED);

        cfg.setCacheConfiguration(accountsCfg);

        cfg.setConnectorConfiguration(null);

        ignite = Ignition.getOrStart(cfg);

        ignite.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override public void start() {
        long start = System.currentTimeMillis();

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

        registerVisorTasks();

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
        router.route("/api/v1/notebooks").handler(this::handleDummy);
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
                sendStatus(ctx, HTTP_UNAUTHORIZED, asyncRes.cause().getMessage());
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
                sendStatus(ctx, HTTP_UNAUTHORIZED, "Invalid email or password: " + asyncRes.cause().getMessage());
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
            .deployVerticle(new WebConsoleServer());
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
            this.argCls = argCls;
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

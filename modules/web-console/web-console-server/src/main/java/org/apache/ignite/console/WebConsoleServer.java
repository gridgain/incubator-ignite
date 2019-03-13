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
import java.util.function.Function;
import java.util.stream.Stream;
import com.fasterxml.jackson.databind.DeserializationFeature;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.bridge.BridgeEventType;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CookieHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.BridgeEvent;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.sstore.ClusteredSessionStore;
import io.vertx.spi.cluster.ignite.IgniteClusterManager;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.common.Addresses;
import org.apache.ignite.console.config.SslConfiguration;
import org.apache.ignite.console.config.WebConsoleConfiguration;
import org.apache.ignite.console.routes.AccountRouter;
import org.apache.ignite.console.routes.AdminRouter;
import org.apache.ignite.console.routes.AgentDownloadRouter;
import org.apache.ignite.console.routes.ConfigurationsRouter;
import org.apache.ignite.console.routes.NotebooksRouter;
import org.apache.ignite.console.routes.RestApiRouter;
import org.apache.ignite.console.services.AccountsService;
import org.apache.ignite.console.services.AdminService;
import org.apache.ignite.console.services.ConfigurationsService;
import org.apache.ignite.console.services.NotebooksService;
import org.apache.ignite.internal.util.typedef.F;

import static java.net.HttpURLConnection.HTTP_MOVED_PERM;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.console.common.Utils.jksOptions;
import static org.apache.ignite.console.common.Utils.origin;

/**
 * Web Console server.
 */
public class WebConsoleServer extends AbstractVerticle {
    /** */
    private static final String VISOR_IGNITE = "org.apache.ignite.internal.visor.";

    static {
        Json.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /** */
    protected final Map<String, VisorTaskDescriptor> visorTasks = new ConcurrentHashMap<>();

    /** */
    protected final Map<String, JsonObject> clusters = new ConcurrentHashMap<>();

    /** */
    protected WebConsoleConfiguration cfg;

    /** */
    protected Ignite ignite;

    /** */
    protected List<RestApiRouter> restRoutes;

    /** {@inheritDoc} */
    @Override public void start(Future<Void> startFut) {
        if (getVertx() instanceof VertxInternal) {
            ClusterManager mgmt = ((VertxInternal)getVertx()).getClusterManager();

            if (mgmt instanceof IgniteClusterManager)
                ignite = ((IgniteClusterManager)mgmt).getIgniteInstance();
        }

        if (ignite == null)
            startFut.fail(new IllegalStateException("Verticle deployed not in cluster mode, failed to find Ignite node"));

        ConfigRetriever.create(vertx, buildConfigRetrieverOptions())
            .setConfigurationProcessor(new HierarchicalConfigurationProcessor())
            .getConfig(cfgRes -> {
                try {
                    if (cfgRes.failed())
                        throw cfgRes.cause();

                    cfg = cfgRes.result().mapTo(WebConsoleConfiguration.class);

                    registerServices();

                    startHttpServer();

                    startFut.complete();
                }
                catch (Throwable e) {
                    startFut.fail(e);
                }
            });
    }

    /**
     * @return Configuration retriever options.
     */
    protected ConfigRetrieverOptions buildConfigRetrieverOptions() {
        ConfigRetrieverOptions cfgOpts = new ConfigRetrieverOptions();

        cfgOpts.addStore(new ConfigStoreOptions()
            .setType("env"));

        String cfgPath = config().getString("configPath");

        if (!F.isEmpty(cfgPath)) {
            cfgOpts.addStore(new ConfigStoreOptions()
                .setType("file")
                .setFormat("properties")
                .setConfig(new JsonObject().put("path", cfgPath))
            );
        }

        return cfgOpts;
    }

    /**
     * @throws Exception If failed to start HTTP server.
     */
    protected void startHttpServer() throws Exception {
        SockJSHandler sockJsHnd = SockJSHandler.create(vertx);

        BridgeOptions allAccessOptions =
            new BridgeOptions()
                .addInboundPermitted(new PermittedOptions())
                .addOutboundPermitted(new PermittedOptions());

        sockJsHnd.bridge(allAccessOptions, this::handleNodeVisorMessages);

        SslConfiguration sslCfg = cfg.getSslConfiguration();

        boolean ssl = sslCfg != null && sslCfg.isEnabled();

        int port = cfg.getPort();

        // Add redirect to HTTPS.
        if (ssl & port != 80) {
            vertx
                .createHttpServer()
                .requestHandler(req -> {
                    String origin = origin(req).replace("http:", "https:");

                    if (port != 443)
                        origin += ":" + port;

                    req.response()
                        .setStatusCode(HTTP_MOVED_PERM)
                        .setStatusMessage("Server requires HTTPS")
                        .putHeader(HttpHeaders.LOCATION, origin)
                        .end();
                })
                .listen(80);
        }

        Router router = Router.router(vertx);

        router.route().handler(CookieHandler.create());
        router.route().handler(BodyHandler.create());
        router.route().handler(SessionHandler.create(ClusteredSessionStore.create(vertx)));

        if (!F.isEmpty(cfg.getWebRoot()))
            router.route().handler(StaticHandler.create(cfg.getWebRoot()));

        router.route("/eventbus/*").handler(sockJsHnd);

        registerRestRoutes(router);

        registerVisorTasks();

        HttpServerOptions httpOpts = new HttpServerOptions()
            .setCompressionSupported(true)
            .setPerMessageWebsocketCompressionSupported(true);

        if (ssl) {
            httpOpts.setSsl(true);

            JksOptions jks = jksOptions(sslCfg.getKeyStore(), sslCfg.getKeyStorePassword());

            if (jks != null)
                httpOpts.setKeyStoreOptions(jks);

            jks = jksOptions(sslCfg.getTrustStore(), sslCfg.getTrustStorePassword());

            if (jks != null)
                httpOpts.setTrustStoreOptions(jks);

            String ciphers = sslCfg.getCipherSuites();

            if (!F.isEmpty(ciphers)) {
                Arrays
                    .stream(ciphers.split(","))
                    .map(String::trim)
                    .forEach(httpOpts::addEnabledCipherSuite);
            }

            httpOpts
                .setClientAuth(sslCfg.isClientAuth() ? ClientAuth.REQUIRED : ClientAuth.REQUEST);
        }

        vertx
            .createHttpServer(httpOpts)
            .requestHandler(router)
            .listen(port);

        ignite.log().info("Web Console server started.");
    }

    /**
     * Register event bus consumers.
     */
    protected void registerServices() {
        vertx.eventBus().consumer(Addresses.CLUSTER_TOPOLOGY, this::handleClusterTopology);

        AccountsService accountsSvc = new AccountsService(ignite).install(vertx);
        ConfigurationsService cfgsSvc = new ConfigurationsService(ignite).install(vertx);
        NotebooksService notebooksSvc = new NotebooksService(ignite).install(vertx);

        new AdminService(ignite, accountsSvc, cfgsSvc, notebooksSvc).install(vertx);
    }

    /**
     * TODO IGNITE-5617 Replace with REAL routes!
     * @param router Router.
     */
    protected void registerDummyRoutes(Router router) {
        router.route("/api/v1/activation/resend").handler(this::handleDummy);
        router.route("/api/v1/activities/page").handler(this::handleDummy);

        router.route("/api/v1/admin").handler(this::handleDummy);
        router.route("/api/v1/profile").handler(this::handleDummy);
        router.route("/api/v1/demo").handler(this::handleDummy);

        router.route("/api/v1/downloads").handler(this::handleDummy);
        router.post("/api/v1/activities/page").handler(this::handleDummy);
    }

    /**
     * Register REST routes.
     *
     * @param router Router.
     */
    protected void registerRestRoutes(Router router) {
        registerDummyRoutes(router);

        restRoutes = Arrays.asList(
            new AccountRouter(ignite, vertx),
            new AdminRouter(ignite, vertx),
            new ConfigurationsRouter(ignite, vertx),
            new NotebooksRouter(ignite, vertx),
            new AgentDownloadRouter(ignite, vertx, cfg)
        );

        restRoutes.forEach(route -> route.install(router));
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
     * @param msg Message to send.
     */
    private void sendStatus(RoutingContext ctx, int status, String msg) {
        ctx.response().setStatusCode(status).end(msg);
    }

    /**
     * @param ctx Context
     */
    protected void handleDummy(RoutingContext ctx) {
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
     *
     * TODO IGNITE-5617 Move to separate class?
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

    /**
     * Configuration processor that convert flat JSON to hierarchical.
     */
    private static class HierarchicalConfigurationProcessor implements Function<JsonObject, JsonObject> {
        /** {@inheritDoc} */
        @Override public JsonObject apply(JsonObject src) {
            return src
                .stream()
                .map(entry -> {
                    String key = entry.getKey();
                    Object val = entry.getValue();

                    if (val instanceof String)
                        val = tryParse((String)val);

                    List<String> paths = asList(key.split("\\."));

                    JsonObject json = new JsonObject();

                    if (paths.size() == 1)
                        json.put(key, val);
                    else
                        json.put(paths.get(0), toJson(paths.subList(1, paths.size()), val));

                    return json;
                })
                .reduce((json, other) -> json.mergeIn(other, true))
                .orElse(new JsonObject());
        }

        /**
         * Convert to hierarchical JSON.
         * @param paths Path.
         * @param val Property value.
         * @return JSON.
         */
        private JsonObject toJson(List<String> paths, Object val) {
            if (paths.isEmpty())
                return new JsonObject();

            if (paths.size() == 1)
                return new JsonObject().put(paths.get(0), val);

            String path = paths.get(0);

            JsonObject jsonVal = toJson(paths.subList(1, paths.size()), val);

            return new JsonObject().put(path, jsonVal);
        }

        /**
         * @param raw Raw value.
         * @return Parsed value.
         */
        private Object tryParse(String raw) {
            if (raw.contains(",")) {
                return Stream.of(raw.split(","))
                    .map(this::tryParse)
                    .collect(collectingAndThen(toList(), JsonArray::new));
            }

            if ("true".equals(raw))
                return true;

            if ("false".equals(raw))
                return false;

            if (raw.matches("^\\d+\\.\\d+$"))
                return Double.parseDouble(raw);

            if (raw.matches("^\\d+$"))
                return Integer.parseInt(raw);

            return raw;
        }
    }
}

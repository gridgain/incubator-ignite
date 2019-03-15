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

package org.apache.ignite.console.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.bridge.BridgeEventType;
import io.vertx.ext.web.handler.sockjs.BridgeEvent;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.console.WebConsoleServer;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.request.GridRestTaskRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestTopologyRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.compute.VisorGatewayTask;

import static org.apache.ignite.console.common.Utils.uuidParam;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.EXE;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;

/**
 * Service to handle accounts.
 */
public class AgentService extends AbstractService {
    /** */
    protected final Map<String, VisorTaskDescriptor> visorTasks = new ConcurrentHashMap<>();

    /** */
    private static final String VISOR_IGNITE = "org.apache.ignite.internal.visor.";

    /**
     * @param ignite Ignite.
     */
    public AgentService(Ignite ignite) {
        super(ignite);

        registerVisorTasks();
    }

    /** {@inheritDoc} */
    @Override public AgentService install(Vertx vertx) {


        return this;
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
     * @param shortName Class short name.
     * @return Full class name.
     */
    protected String igniteVisor(String shortName) {
        return VISOR_IGNITE + shortName;
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
     * Handle REST commands.
     *
     * @param msg Message with data.
     */
    protected JsonObject rest(JsonObject msg) {
        try {
            JsonObject params = msg.getJsonObject("params"); // TODO IGNITE-5617 check for NPE?

            GridRestCommand cmd = GridRestCommand.fromKey(params.getString("cmd"));

            if (cmd == null)
                throw new IllegalArgumentException("Unsupported REST command: " + params);

            switch (cmd) {
                case TOPOLOGY:
                case NODE:
                    GridRestTopologyRequest req = new GridRestTopologyRequest();
                    req.command(cmd);

                    req.includeMetrics(params.getBoolean("mtr", false));
                    req.includeAttributes(params.getBoolean("attr", false));
                    req.includeCaches(params.getBoolean("caches", false));

                    req.nodeIp(params.getString("ip", null));

                    if (params.containsKey("id"))
                        req.nodeId(uuidParam(params, "id"));

                    return new JsonObject(Json.encode(topHnd.handleAsync(req).get()));

                default:
                    throw new IllegalArgumentException("Unsupported REST command: " + params);
            }
        }
        catch (Throwable e) {
            U.error(ignite.log(), "Failed to process HTTP request", e);

            return new JsonObject(Json.encode(new GridRestResponse(STATUS_FAILED, e.getMessage())));
        }
    }

    /**
     * Handle Visor commands.
     *
     * @param msg Message with data.
     */
    protected JsonObject visorTask(JsonObject msg) {
        try {
            JsonObject params = msg.getJsonObject("params"); // TODO IGNITE-5617 check for NPE?
            
            GridRestTaskRequest req = new GridRestTaskRequest();

            req.command(EXE);
            req.taskId(params.getString("id"));
            req.taskName(params.getString("name"));
            
            req.async(params.getBoolean("async", false));
            req.timeout(params.getLong("timeout", 0L));

            WebConsoleServer.VisorTaskDescriptor desc = visorTasks.get(taskId);

            req.params(values(null, "p", params));



            return new JsonObject(Json.encode(taskHnd.handleAsync(req).get()));
        }
        catch (Throwable e) {
            U.error(ignite.log(), "Failed to process HTTP request", e);

            return new JsonObject(Json.encode(new GridRestResponse(STATUS_FAILED, e.getMessage())));
        }

            JsonObject params = msg.getJsonObject("params"); // TODO IGNITE-5617 check for NPE?

            String taskId = params.getString("taskId");



            JsonArray z = params.getJsonArray("args");

            int sz = z.size() + desc.getArgumentsClasses().length + 2;

            JsonObject execParams = WebConsoleServer.prepareNodeVisorParams(desc, params.getString("nids"), z);

            List<Object> args = new ArrayList<>(sz);

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
}

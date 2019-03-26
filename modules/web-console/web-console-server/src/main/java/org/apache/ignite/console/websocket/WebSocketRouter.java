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

package org.apache.ignite.console.websocket;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.rest.RestApiController;
import org.apache.ignite.console.services.VisorTaskDescriptor;
import org.apache.ignite.internal.util.typedef.F;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.PongMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import static org.apache.ignite.console.util.JsonUtils.getString;
import static org.apache.ignite.console.util.JsonUtils.paramsFromJson;
import static org.apache.ignite.console.websocket.Utils.encodeJson;
import static org.apache.ignite.console.websocket.Utils.toAgentInfo;
import static org.apache.ignite.console.websocket.Utils.toBrowserInfo;
import static org.apache.ignite.console.websocket.Utils.toWsEvt;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_INFO;
import static org.apache.ignite.console.websocket.WebSocketEvents.BROWSER_INFO;
import static org.apache.ignite.console.websocket.WebSocketEvents.CLUSTER_TOPOLOGY;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_REST;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_VISOR;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_DRIVERS;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_METADATA;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_SCHEMAS;

/**
 * Router for requests from web sockets.
 */
@Component
public class WebSocketRouter extends TextWebSocketHandler {
    /** */
    private static final Logger log = LoggerFactory.getLogger(RestApiController.class);

    /** */
    private static final String VISOR_IGNITE = "org.apache.ignite.internal.visor.";

    /** */
    private final WebSocketSessions wss;

    /** */
    private final Map<String, WebSocketSession> requests;

    /** */
    private final Map<String, VisorTaskDescriptor> visorTasks;

    /**
     * @param wss Websocket sessions.
     */
    public WebSocketRouter(WebSocketSessions wss) {
        this.wss = wss;

        requests = new ConcurrentHashMap<>();
        visorTasks = new ConcurrentHashMap<>();
    }

    /**
     * @param ws Websocket.
     * @param msg Incoming message.
     */
    private void handleAgentEvents(WebSocketSession ws, TextMessage msg) {
        try {
            WebSocketEvent evt = toWsEvt(msg.getPayload());

            switch (evt.getEventType()) {
                case AGENT_INFO:
                    AgentInfo agentInfo = toAgentInfo(evt.getPayload());

                    log.info("Agent connected: " + agentInfo.getAgentId());

                    break;

                // TODO IGNITE-5617: implement broadcasting of topology events.
                case CLUSTER_TOPOLOGY:
                    wss.broadcastToBrowsers(evt);

                    break;

                default:
                    WebSocketSession browserWs = requests.remove(evt.getRequestId());

                    if (browserWs != null)
                        browserWs.sendMessage(new TextMessage(encodeJson(evt)));
                    else
                        log.warn("Failed to send event to browser: " + evt);
            }
        }
        catch (Throwable e) {
            log.error("Failed to process event from web console agent [socket=" + ws + ", msg=" + msg + "]", e);
        }
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

    private void prepareVisorTaskParams(WebSocketEvent evt) throws Exception {
        Map<String, Object> params = paramsFromJson(evt.getPayload());

        String taskId = getString(params, "taskId", "");

        if (F.isEmpty(taskId))
            throw new IllegalStateException("Task ID not found");

        VisorTaskDescriptor desc = visorTasks.get(taskId);

        String nids = getString(params, "nids", "");

        String args = params.get("args");
        // , params.getJsonArray("args")

        JsonObject exeParams = prepareNodeVisorParams(desc, params.getString("nids"), params.getJsonArray("args"));

        body.put("params", exeParams);

        msg.put("body", body);

        Map<String, Object> exeParams = new LinkedHashMap<String, Object>();

        exeParams.put("cmd", "exe");
        exeParams.put("name", "org.apache.ignite.internal.visor.compute.VisorGatewayTask");
        exeParams.put("p1", nids);
        exeParams.put("p2", desc.getTaskClass());

        AtomicInteger idx = new AtomicInteger(3);

        Arrays.stream(desc.getArgumentsClasses()).forEach(arg ->  exeParams.put("p" + idx.getAndIncrement(), arg));

        args.forEach(arg -> exeParams.put("p" + idx.getAndIncrement(), arg));
    }

    /**
     * @param ws Websocket.
     * @param msg Incoming message.
     */
    private void handleBrowserEvents(WebSocketSession ws, TextMessage msg) {
        try {
            WebSocketEvent evt = toWsEvt(msg.getPayload());

            switch (evt.getEventType()) {
                case BROWSER_INFO:
                    BrowserInfo browserInfo = toBrowserInfo(evt.getPayload());

                    log.info("Browser connected: " + browserInfo.getBrowserId());

                    break;

                case NODE_VISOR:
                    prepareVisorTaskParams(evt);

                case SCHEMA_IMPORT_DRIVERS:
                case SCHEMA_IMPORT_SCHEMAS:
                case SCHEMA_IMPORT_METADATA:
                case NODE_REST:
                    requests.put(evt.getRequestId(), ws);

                    // TODO IGNITE-5617: select correct agent.
                    wss.broadcastToAgents(evt);
            }
        }
        catch (Throwable e) {
            log.error("Failed to process event from browser [socket=" + ws + ", msg=" + msg + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void handleTextMessage(WebSocketSession ws, TextMessage msg) {
        if (AGENTS_PATH.equals(ws.getUri().getPath()))
            handleAgentEvents(ws, msg);
        else
            handleBrowserEvents(ws, msg);
    }

    /** {@inheritDoc} */
    @Override protected void handlePongMessage(WebSocketSession ws, PongMessage msg) {
        try {
            if (log.isTraceEnabled())
                log.trace("Received pong message [socket=" + ws + ", msg=" + msg + "]");
        }
        catch (Throwable e) {
            log.error("Failed to process pong message [socket=" + ws + ", msg=" + msg + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionEstablished(WebSocketSession ws) {
        //Messages will be sent to all users.
        wss.openSession(ws);
    }
}

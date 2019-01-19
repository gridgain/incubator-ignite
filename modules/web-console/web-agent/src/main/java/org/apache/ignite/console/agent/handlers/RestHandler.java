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

package org.apache.ignite.console.agent.handlers;

import java.util.Map;
import io.vertx.core.AbstractVerticle;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.demo.AgentClusterDemo;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;

/**
 * API to translate REST requests to Ignite cluster.
 */
public class RestHandler extends AbstractVerticle {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(RestHandler.class));

    /** */
    private static final String EVENT_NODE_VISOR_TASK = "node:visorTask";

    /** */
    private static final String EVENT_NODE_REST = "node:rest";

    /** */
    private final AgentConfiguration cfg;

    /**
     * @param cfg Config.
     */
    public RestHandler(AgentConfiguration cfg) {
        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    public Object execute(Map<String, Object> args) {
        if (log.isDebugEnabled())
            log.debug("Start parse REST command args: " + args);

        Map<String, Object> params = null;

        if (args.containsKey("params"))
            params = (Map<String, Object>)args.get("params");

        if (!args.containsKey("demo"))
            throw new IllegalArgumentException("Missing demo flag in arguments: " + args);

        boolean demo = (boolean)args.get("demo");

        if (F.isEmpty((String)args.get("token")))
            return RestResult.fail(401, "Request does not contain user token.");

        Map<String, Object> headers = null;

        if (args.containsKey("headers"))
            headers = (Map<String, Object>)args.get("headers");

        try {
            if (demo) {
                if (AgentClusterDemo.getDemoUrl() == null) {
                    AgentClusterDemo.tryStart().await();

                    if (AgentClusterDemo.getDemoUrl() == null)
                        return RestResult.fail(404, "Failed to send request because of embedded node for demo mode is not started yet.");
                }

                return null; // restExecutor.sendRequest(AgentClusterDemo.getDemoUrl(), params, headers);
            }

            return null; // restExecutor.sendRequest(this.cfg.nodeURIs(), params, headers);
        }
        catch (Throwable e) {
            U.error(log, "Failed to execute REST command with parameters: " + params, e);

            return RestResult.fail(404, e.getMessage());
        }
    }
}

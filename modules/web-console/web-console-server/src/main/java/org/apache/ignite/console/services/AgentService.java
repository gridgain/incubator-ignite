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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.console.json.JsonObject;

/**
 * Service to handle requests to Visor tasks.
 */
public class AgentService {

//    /**
//     * @param be Bridge event
//     */
//    protected void handleNodeVisorMessages(BridgeEvent be) {
//        if (be.type() == BridgeEventType.SEND) {
//            JsonObject msg = be.getRawMessage();
//
//            if (msg != null) {
//                String addr = msg.getString("address");
//
//                if ("node:visor".equals(addr)) {
//                    JsonObject body = msg.getJsonObject("body");
//
//                    if (body != null) {
//                        JsonObject params = body.getJsonObject("params");
//
//                        String taskId = params.getString("taskId");
//
//                        if (!F.isEmpty(taskId)) {
//                            VisorTaskDescriptor desc = visorTasks.get(taskId);
//
//                            JsonObject exeParams = prepareNodeVisorParams(desc, params.getString("nids"), params.getJsonArray("args"));
//
//                            body.put("params", exeParams);
//
//                            msg.put("body", body);
//                        }
//                    }
//                }
//            }
//        }
//
//        be.complete(true);
//    }
//

    /**
     * Visor task descriptor.
     *
     * TODO IGNITE-5617 Move to separate class?
     */

}

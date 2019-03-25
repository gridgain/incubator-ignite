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

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.ignite.console.websocket.AgentInfo;
import org.apache.ignite.console.websocket.BrowserInfo;
import org.apache.ignite.console.websocket.WebSocketEvent;

/**
 *
 */
public class Utils {
    /** */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * @param json JSON.
     * @return Web socket event from JSON.
     * @throws IOException If failed.
     */
    public static WebSocketEvent toWsEvt(String json) throws IOException {
        return MAPPER.readValue(json, WebSocketEvent.class);
    }

    /**
     *
     * @param json JSON.
     * @return Agent info from JSON.
     * @throws IOException If failed.
     */
    public static AgentInfo toAgentInfo(String json) throws IOException {
        return MAPPER.readValue(json, AgentInfo.class);
    }

    /**
     * @param json JSON.
     * @return Browser info from JSON.
     * @throws IOException If failed.
     */
    public static BrowserInfo toBrowserInfo(String json) throws IOException {
        return MAPPER.readValue(json, BrowserInfo.class);
    }

    /**
     * @param val Object to serialize to JSON.
     * @return JSON.
     * @throws IOException If failed.
     */
    public static String encodeJson(Object val) throws IOException {
        return MAPPER.writeValueAsString(val);
    }
}

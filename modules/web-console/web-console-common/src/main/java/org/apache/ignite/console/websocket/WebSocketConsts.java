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

/**
 * Contains event bus addresses.
 */
public interface WebSocketConsts {
    /** */
    public static final String AGENTS_PATH = "/agents";

    /** */
    public static final String BROWSERS_PATH = "/browsers";

    /** */
    public static final String ERROR = "error";

    /** */
    public static final String AGENT_INFO = "agent:info";

    /** */
    public static final String AGENT_RESET_TOKEN = "agent:reset:token";

    /** */
    public static final String AGENT_STATUS = "agent:status";

    /** */
    public static final String SCHEMA_IMPORT_DRIVERS = "schemaImport:drivers";

    /** */
    public static final String SCHEMA_IMPORT_SCHEMAS = "schemaImport:schemas";

    /** */
    public static final String SCHEMA_IMPORT_METADATA = "schemaImport:metadata";

    /** */
    public static final String NODE_REST = "node:rest";

    /** */
    public static final String NODE_VISOR = "node:visor";

    /** */
    public static final String CLUSTER_TOPOLOGY = "cluster:topology";

    /** */
    public static final String CLUSTER_DISCONNECTED = "cluster:disconnected";

    /** */
    public static final String EVENT_LOG_WARNING = "log:warn";
}

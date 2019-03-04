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

package org.apache.ignite.console.common;

/**
 * Contains event bus addresses.
 */
public interface Addresses {
    /** */
    public static final String AGENTS_STATUS = "agents:stat";

    /** */
    public static final String NODE_REST = "node:rest";

    /** */
    public static final String NODE_VISOR = "node:visor";

    /** */
    public static final String CLUSTER_TOPOLOGY = "cluster:topology";

    /** */
    public static final String NOTEBOOK_LIST = "notebook:list";

    /** */
    public static final String NOTEBOOK_SAVE = "notebook:save";

    /** */
    public static final String NOTEBOOK_DELETE = "notebook:delete";

    /** */
    public static final String ACCOUNT_GET_BY_ID = "account:get:byId";

    /** */
    public static final String ACCOUNT_GET_BY_EMAIL = "account:get:byEmail";

    /** */
    public static final String ACCOUNT_REGISTER = "account:register";

    /** */
    public static final String ACCOUNT_LIST = "account:list";

    /** */
    public static final String ACCOUNT_SAVE = "account:save";

    /** */
    public static final String ACCOUNT_DELETE = "account:delete";

    /** */
    public static final String ACCOUNT_TOGGLE = "account:toggle";

    /** */
    public static final String CONFIGURATION_LOAD = "configuration:load";

    /** */
    public static final String CONFIGURATION_LOAD_SHORT_CLUSTERS = "configuration:load:shortClusters";

    /** */
    public static final String CONFIGURATION_LOAD_CLUSTER = "configuration:load:cluster";

    /** */
    public static final String CONFIGURATION_LOAD_SHORT_CACHES = "configuration:load:shortCaches";

    /** */
    public static final String CONFIGURATION_LOAD_SHORT_MODELS = "configuration:load:shortModels";

    /** */
    public static final String CONFIGURATION_LOAD_SHORT_IGFSS = "configuration:load:shortIgfss";

    /** */
    public static final String CONFIGURATION_LOAD_CACHE = "configuration:load:cache";

    /** */
    public static final String CONFIGURATION_LOAD_MODEL = "configuration:load:model";

    /** */
    public static final String CONFIGURATION_LOAD_IGFS = "configuration:load:igfs";

    /** */
    public static final String CONFIGURATION_SAVE_CLUSTER_ADVANCED = "configuration:save:cluster:advanced";

    /** */
    public static final String CONFIGURATION_SAVE_CLUSTER_BASIC = "configuration:save:cluster:basic";

    /** */
    public static final String CONFIGURATION_DELETE_CLUSTER = "configuration:delete:cluster";
}

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

import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Cache;
import org.apache.ignite.console.dto.Cluster;
import org.apache.ignite.console.dto.Igfs;
import org.apache.ignite.console.dto.Model;

/**
 * Service to handle notebooks.
 */
public class ConfigurationsService extends AbstractService {
    /** */
    private final Table<Cluster> clustersTbl;

    /** */
    private final Table<Cache> cachesTbl;

    /** */
    private final Table<Model> modelsTbl;

    /** */
    private final Table<Igfs> igfssTbl;

    /** */
    private final OneToManyIndex clustersIdx;

    /** */
    private final OneToManyIndex cachesIdx;

    /** */
    private final OneToManyIndex modelsIdx;

    /** */
    private final OneToManyIndex igfssIdx;

    /**
     * @param ignite Ignite.
     */
    public ConfigurationsService(Ignite ignite) {
        super(ignite);

        clustersTbl = new Table<Cluster>(ignite, "wc_account_clusters")
            .addUniqueIndex(Cluster::name, (cluster) -> "Cluster '" + cluster + "' already exits");

        cachesTbl = new Table<>(ignite, "wc_cluster_caches");
        modelsTbl = new Table<>(ignite, "wc_cluster_models");
        igfssTbl = new Table<>(ignite, "wc_cluster_igfss");
        clustersIdx = new OneToManyIndex(ignite, "wc_account_clusters_idx");

        cachesIdx = new OneToManyIndex(ignite, "wc_cluster_caches_idx");
        modelsIdx = new OneToManyIndex(ignite, "wc_cluster_models_idx");
        igfssIdx = new OneToManyIndex(ignite, "wc_cluster_igfss_idx");
    }

    /** {@inheritDoc} */
    @Override protected void initialize() {
        clustersIdx.cache();
        cachesIdx.cache();
        modelsIdx.cache();
        igfssIdx.cache();

    }
}

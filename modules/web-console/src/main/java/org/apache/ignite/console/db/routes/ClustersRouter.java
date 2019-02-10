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

package org.apache.ignite.console.db.routes;

import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.console.db.dto.Cluster;
import org.apache.ignite.console.db.index.OneToManyIndex;
import org.apache.ignite.console.db.index.UniqueIndex;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;

/**
 * Router to handle REST API for clusters.
 */
@SuppressWarnings("JavaAbbreviationUsage")
public class ClustersRouter extends AbstractRouter<UUID, Cluster> {
    /** */
    public static final String CLUSTERS_CACHE = "wc_clusters";

    /** */
    private final OneToManyIndex accountClustersIdx;

    /** */
    private final UniqueIndex uniqueClusterNameIdx;

    /** */
    private final OneToManyIndex clusterCachesIdx;

    /** */
    private final OneToManyIndex clusterModelsIdx;

    /** */
    private final OneToManyIndex clusterIgfsIdx;

    /**
     * @param ignite Ignite.
     */
    public ClustersRouter(Ignite ignite) {
        super(ignite, CLUSTERS_CACHE);

        accountClustersIdx = new OneToManyIndex(ignite, "account", "clusters");
        uniqueClusterNameIdx = new UniqueIndex(ignite, "uniqueClusterNameIdx");
        clusterCachesIdx = new OneToManyIndex(ignite, "cluster", "caches");
        clusterModelsIdx = new OneToManyIndex(ignite, "cluster", "models");
        clusterIgfsIdx = new OneToManyIndex(ignite, "cluster", "igfss");
    }

    /** {@inheritDoc} */
    @Override public void prepare() {
        super.prepare();

        accountClustersIdx.prepare();
        uniqueClusterNameIdx.prepare();
    }

    /**
     *
     * @param user
     * @param rawData
     * @return
     */
    private Cluster put(JsonObject user, JsonObject rawData) {
        UUID userId = getUserId(user);

        // rawData = Schemas.sanitize(Cluster.class, rawData);

        UUID clusterId = ensureId(rawData);

        String clusterName = rawData.getString("name");

        if (F.isEmpty(clusterName))
            throw new IllegalStateException("Cluster name is empty");

        Cluster cluster = new Cluster(clusterId, null, clusterName, "Multicast", rawData.encode());

        try(Transaction tx = txStart()) {
            UUID prevId = uniqueClusterNameIdx.getAndPutIfAbsent(userId, clusterName, clusterId);

            if (prevId != null && !clusterId.equals(prevId))
                throw new IllegalStateException("Cluster with name '" + clusterName + "' already exits");

            accountClustersIdx.put(userId, clusterId);

            cache().put(clusterId, cluster);

            tx.commit();
        }

        return cluster;
    }

    /**
     *
     * @param user
     * @param rawData
     * @return
     */
    private boolean remove(JsonObject user, JsonObject rawData) {
        UUID userId = getUserId(user);

        UUID clusterId = getId(rawData);

        if (clusterId == null)
            throw new IllegalStateException("Cluster ID not found");

        boolean removed = false;

        try(Transaction tx = txStart()) {
            IgniteCache<UUID, Cluster> cache = cache();

            Cluster cluster = cache.getAndRemove(clusterId);

            if (cluster != null) {
                accountClustersIdx.removeChild(userId, clusterId);
                uniqueClusterNameIdx.remove(userId, cluster.name());
                clusterCachesIdx.remove(clusterId);
                clusterModelsIdx.remove(clusterId);
                clusterIgfsIdx.remove(clusterId);

                removed = true;
            }

            tx.commit();
        }

        return removed;
    }

    /**
     * Load clusters short list.
     *
     * @param ctx Context.
     */
    public void loadShortList(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID userId = getUserId(user.principal());

                try(Transaction tx = txStart()) {
                    TreeSet<UUID> clusterIds = accountClustersIdx.getIds(userId);

                    Collection<Cluster> clusters = cache().getAll(clusterIds).values();

                    tx.commit();

                    JsonArray shortList = new JsonArray();

                    clusters.forEach(cluster -> {
                        // TODO IGNITE-5617 get counts...
                        int cachesCount = 0;
                        int modelsCount = 0;
                        int igfsCount = 0;

                        shortList.add(new JsonObject()
                            .put("_id", cluster._id())
                            .put("name", cluster.name())
                            .put("discovery", cluster.discovery())
                            .put("cachesCount", cachesCount)
                            .put("modelsCount", modelsCount)
                            .put("igfsCount", igfsCount));
                    });

                    sendResult(ctx, shortList.toBuffer());
                }
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to load clusters", e);
            }
        }
    }

    /**
     * Save cluster.
     *
     * @param ctx Context.
     */
    public void save(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID userId = getUserId(user.principal());

                JsonObject rawData = ctx.getBodyAsJson().getJsonObject("cluster");
                
                // rawData = Schemas.sanitize(Cluster.class, rawData);

                UUID clusterId = getId(rawData);
                
                if (clusterId == null)
                    throw new IllegalStateException("Cluster ID not found");

                String name = rawData.getString("name");

                if (F.isEmpty(name))
                    throw new IllegalStateException("Cluster name is empty");

                String discovery = rawData.getJsonObject("discovery").getString("kind");

                if (F.isEmpty(name))
                    throw new IllegalStateException("Cluster discovery not found");

                Cluster cluster = new Cluster(clusterId, null, name, discovery, rawData.encode());

                try(Transaction tx = txStart()) {
                    UUID prevId = uniqueClusterNameIdx.getAndPutIfAbsent(userId, name, clusterId);

                    if (prevId != null && !clusterId.equals(prevId))
                        throw new IllegalStateException("Cluster with name '" + name + "' already exits");

                    accountClustersIdx.put(userId, clusterId);

                    cache().put(clusterId, cluster);

                    tx.commit();
                }

                sendResult(ctx, rawData);
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to save cluster", e);
            }
        }

    }
}

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

package org.apache.ignite.client;

import org.apache.ignite.cache.*;

import java.util.*;

/** Cache configuration. */
public final class CacheClientConfiguration {
    /** Cache name. */
    private String name;

    /** Atomicity mode. */
    private CacheAtomicityMode atomicityMode = CacheAtomicityMode.ATOMIC;

    /** Backups. */
    private int backups = 0;

    /** Cache mode. */
    private CacheMode cacheMode = CacheMode.PARTITIONED;

    /** Eager TTL flag. */
    private boolean eagerTtl = true;

    /** Group name. */
    private String grpName = null;

    /** Default lock timeout. */
    private long dfltLockTimeout = 0;

    /** Partition loss policy. */
    private PartitionLossPolicy partLossPlc = PartitionLossPolicy.IGNORE;

    /** Read from backup. */
    private boolean readFromBackup = true;

    /** Rebalance batch size. */
    private int rebalanceBatchSize = 512 * 1024; // 512K

    /** Rebalance batches prefetch count. */
    private long rebalanceBatchesPrefetchCnt = 2;

    /** Rebalance delay. */
    private long rebalanceDelay = 0;

    /** Rebalance mode. */
    private CacheRebalanceMode rebalanceMode = CacheRebalanceMode.ASYNC;

    /** Rebalance order. */
    private int rebalanceOrder = 0;

    /** Rebalance throttle. */
    private long rebalanceThrottle = 0;

    /** Rebalance timeout. */
    private long rebalanceTimeout = 10000;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode writeSynchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;

    /** Copy on read. */
    private boolean cpOnRead = true;

    /** Data region name. */
    private String dataRegionName;

    /** Statistics enabled. */
    private boolean statisticsEnabled = false;

    /** Max concurrent async operations. */
    private int maxConcurrentAsyncOperations = 500;

    /** Max query iterators count. */
    private int maxQryIteratorsCnt = 1024;

    /** Onheap cache enabled. */
    private boolean onheapCacheEnabled = false;

    /** Query detail metrics size. */
    private int qryDetailMetricsSize = 0;

    /** Query parallelism. */
    private int qryParallelism = 1;

    /** Sql escape all. */
    private boolean sqlEscapeAll = false;

    /** Sql index max inline size. */
    private int sqlIdxMaxInlineSize = -1;

    /** Sql schema. */
    private String sqlSchema;

    /** Key config. */
    private Collection<CacheKeyConfiguration> keyCfg = Collections.emptyList();

    /** Query entities. */
    private Collection<QueryEntity> qryEntities = Collections.emptyList();

    /**
     * @param name Cache name.
     */
    public CacheClientConfiguration(String name) {
        this.name = name;
    }

    /**
     * @return Cache name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name New cache name.
     */
    public CacheClientConfiguration setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * @return Cache atomicity mode.
     */
    public CacheAtomicityMode getAtomicityMode() {
        return atomicityMode;
    }

    /**
     * @param atomicityMode New Atomicity mode.
     */
    public CacheClientConfiguration setAtomicityMode(CacheAtomicityMode atomicityMode) {
        this.atomicityMode = atomicityMode;

        return this;
    }

    /**
     * @return Number of backups.
     */
    public int getBackups() {
        return backups;
    }

    /**
     * @param backups New number of backups.
     */
    public CacheClientConfiguration setBackups(int backups) {
        this.backups = backups;

        return this;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode getCacheMode() {
        return cacheMode;
    }

    /**
     * @param cacheMode New cache mode.
     */
    public CacheClientConfiguration setCacheMode(CacheMode cacheMode) {
        this.cacheMode = cacheMode;

        return this;
    }

    /**
     * Gets flag indicating whether expired cache entries will be eagerly removed from cache.
     * If there is at least one cache configured with this flag set to {@code true}, Ignite
     * will create a single thread to clean up expired entries in background. When flag is
     * set to {@code false}, expired entries will be removed on next entry access.
     *
     * @return Flag indicating whether Ignite will eagerly remove expired entries.
     */
    public boolean isEagerTtl() {
        return eagerTtl;
    }

    /**
     * @param eagerTtl {@code True} if Ignite should eagerly remove expired cache entries.
     */
    public CacheClientConfiguration setEagerTtl(boolean eagerTtl) {
        this.eagerTtl = eagerTtl;

        return this;
    }

    /**
     * Gets the cache group name.
     * <p>
     * Caches with the same group name share single underlying 'physical' cache (partition set),
     * but are logically isolated. Grouping caches reduces overall overhead, since internal data structures are shared.
     * </p>
     */
    public String getGroupName() {
        return grpName;
    }

    /**
     * @param newVal Group name.
     */
    public CacheClientConfiguration setGroupName(String newVal) {
        grpName = newVal;

        return this;
    }

    /**
     * Gets default lock acquisition timeout. {@code 0} and means that lock acquisition will never timeout.
     */
    public long getDefaultLockTimeout() {
        return dfltLockTimeout;
    }

    /**
     * @param dfltLockTimeout Default lock timeout.
     */
    public CacheClientConfiguration setDefaultLockTimeout(long dfltLockTimeout) {
        this.dfltLockTimeout = dfltLockTimeout;

        return this;
    }

    /**
     * Gets partition loss policy. This policy defines how Ignite will react to a situation when all nodes for
     * some partition leave the cluster.
     */
    public PartitionLossPolicy getPartitionLossPolicy() {
        return partLossPlc;
    }

    /**
     * @param newVal Partition loss policy.
     */
    public CacheClientConfiguration setPartitionLossPolicy(PartitionLossPolicy newVal) {
        partLossPlc = newVal;

        return this;
    }

    /**
     * Gets flag indicating whether data can be read from backup.
     * If {@code false} always get data from primary node (never from backup).
     */
    public boolean isReadFromBackup() {
        return readFromBackup;
    }

    /**
     * @param readFromBackup Read from backup.
     */
    public CacheClientConfiguration setReadFromBackup(boolean readFromBackup) {
        this.readFromBackup = readFromBackup;

        return this;
    }

    /**
     * Gets size (in number bytes) to be loaded within a single rebalance message.
     * Rebalancing algorithm will split total data set on every node into multiple
     * batches prior to sending data.
     */
    public int getRebalanceBatchSize() {
        return rebalanceBatchSize;
    }

    /**
     * @param rebalanceBatchSize Rebalance batch size.
     */
    public CacheClientConfiguration setRebalanceBatchSize(int rebalanceBatchSize) {
        this.rebalanceBatchSize = rebalanceBatchSize;

        return this;
    }

    /**
     * To gain better rebalancing performance supplier node can provide more than one batch at rebalancing start and
     * provide one new to each next demand request.
     *
     * Gets number of batches generated by supply node at rebalancing start.
     * Minimum is 1.
     */
    public long getRebalanceBatchesPrefetchCount() {
        return rebalanceBatchesPrefetchCnt;
    }

    /**
     * @param rebalanceBatchesPrefetchCnt Rebalance batches prefetch count.
     */
    public CacheClientConfiguration setRebalanceBatchesPrefetchCount(long rebalanceBatchesPrefetchCnt) {
        this.rebalanceBatchesPrefetchCnt = rebalanceBatchesPrefetchCnt;

        return this;
    }

    /**
     * Gets delay in milliseconds upon a node joining or leaving topology (or crash) after which rebalancing
     * should be started automatically. Rebalancing should be delayed if you plan to restart nodes
     * after they leave topology, or if you plan to start multiple nodes at once or one after another
     * and don't want to repartition and rebalance until all nodes are started.
     * <p>
     * Default value is {@code 0} which means that repartitioning and rebalancing will start
     * immediately upon node leaving topology. If {@code -1} is returned, then rebalancing
     * will only be started manually.
     * </p>
     */
    public long getRebalanceDelay() {
        return rebalanceDelay;
    }

    /**
     * @param rebalanceDelay Rebalance delay.
     */
    public CacheClientConfiguration setRebalanceDelay(long rebalanceDelay) {
        this.rebalanceDelay = rebalanceDelay;

        return this;
    }

    /**
     * Gets rebalance mode.
     */
    public CacheRebalanceMode getRebalanceMode() {
        return rebalanceMode;
    }

    /**
     * @param rebalanceMode Rebalance mode.
     */
    public CacheClientConfiguration setRebalanceMode(CacheRebalanceMode rebalanceMode) {
        this.rebalanceMode = rebalanceMode;

        return this;
    }

    /**
     * Gets cache rebalance order. Rebalance order can be set to non-zero value for caches with
     * {@link CacheRebalanceMode#SYNC SYNC} or {@link CacheRebalanceMode#ASYNC ASYNC} rebalance modes only.
     * <p/>
     * If cache rebalance order is positive, rebalancing for this cache will be started only when rebalancing for
     * all caches with smaller rebalance order will be completed.
     * <p/>
     * Note that cache with order {@code 0} does not participate in ordering. This means that cache with
     * rebalance order {@code 0} will never wait for any other caches. All caches with order {@code 0} will
     * be rebalanced right away concurrently with each other and ordered rebalance processes.
     * <p/>
     * If not set, cache order is 0, i.e. rebalancing is not ordered.
     */
    public int getRebalanceOrder() {
        return rebalanceOrder;
    }

    /**
     * @param rebalanceOrder Rebalance order.
     */
    public CacheClientConfiguration setRebalanceOrder(int rebalanceOrder) {
        this.rebalanceOrder = rebalanceOrder;

        return this;
    }

    /**
     * Time in milliseconds to wait between rebalance messages to avoid overloading of CPU or network.
     * When rebalancing large data sets, the CPU or network can get over-consumed with rebalancing messages,
     * which consecutively may slow down the application performance. This parameter helps tune
     * the amount of time to wait between rebalance messages to make sure that rebalancing process
     * does not have any negative performance impact. Note that application will continue to work
     * properly while rebalancing is still in progress.
     * <p>
     * Default value of {@code 0} means that throttling is disabled.
     * </p>
     */
    public long getRebalanceThrottle() {
        return rebalanceThrottle;
    }

    /**
     * @param newVal Rebalance throttle.
     */
    public CacheClientConfiguration setRebalanceThrottle(long newVal) {
        rebalanceThrottle = newVal;

        return this;
    }

    /**
     * Gets rebalance timeout (ms).
     */
    public long getRebalanceTimeout() {
        return rebalanceTimeout;
    }

    /**
     * @param newVal Rebalance timeout.
     */
    public CacheClientConfiguration setRebalanceTimeout(long newVal) {
        rebalanceTimeout = newVal;

        return this;
    }

    /**
     * Gets write synchronization mode. This mode controls whether the main caller should wait for update on other
     * nodes to complete or not.
     */
    public CacheWriteSynchronizationMode getWriteSynchronizationMode() {
        return writeSynchronizationMode;
    }

    /**
     * @param newVal Write synchronization mode.
     */
    public CacheClientConfiguration setWriteSynchronizationMode(CacheWriteSynchronizationMode newVal) {
        writeSynchronizationMode = newVal;

        return this;
    }


    /**
     * @return Copy on read.
     */
    public boolean isCopyOnRead() {
        return cpOnRead;
    }

    /**
     * @param newVal Copy on read.
     */
    public CacheClientConfiguration setCopyOnRead(boolean newVal) {
        cpOnRead = newVal;

        return this;
    }

    /**
     * @return Max concurrent async operations.
     */
    public int getMaxConcurrentAsyncOperations() {
        return maxConcurrentAsyncOperations;
    }

    /**
     * @param newVal Max concurrent async operations.
     */
    public CacheClientConfiguration setMaxConcurrentAsyncOperations(int newVal) {
        maxConcurrentAsyncOperations = newVal;

        return this;
    }

    /**
     * @return Data region name.
     */
    public String getDataRegionName() {
        return dataRegionName;
    }

    /**
     * @param newVal Data region name.
     */
    public CacheClientConfiguration setDataRegionName(String newVal) {
        dataRegionName = newVal;

        return this;
    }

    /**
     * @return Statistics enabled.
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * @param newVal Statistics enabled.
     */
    public CacheClientConfiguration setStatisticsEnabled(boolean newVal) {
        statisticsEnabled = newVal;

        return this;
    }

    /**
     * @return Max query iterators count.
     */
    public int getMaxQueryIteratorsCount() {
        return maxQryIteratorsCnt;
    }

    /**
     * @param newVal Max query iterators count.
     */
    public CacheClientConfiguration setMaxQueryIteratorsCount(int newVal) {
        maxQryIteratorsCnt = newVal;

        return this;
    }

    /**
     * @return Onheap cache enabled.
     */
    public boolean isOnheapCacheEnabled() {
        return onheapCacheEnabled;
    }

    /**
     * @param newVal Onheap cache enabled.
     */
    public CacheClientConfiguration setOnheapCacheEnabled(boolean newVal) {
        onheapCacheEnabled = newVal;

        return this;
    }

    /**
     * @return Query detail metrics size.
     */
    public int getQueryDetailMetricsSize() {
        return qryDetailMetricsSize;
    }

    /**
     * @param newVal Query detail metrics size.
     */
    public CacheClientConfiguration setQueryDetailMetricsSize(int newVal) {
        qryDetailMetricsSize = newVal;

        return this;
    }

    /**
     * @return Query parallelism.
     */
    public int getQueryParallelism() {
        return qryParallelism;
    }

    /**
     * @param newVal Query parallelism.
     */
    public CacheClientConfiguration setQueryParallelism(int newVal) {
        qryParallelism = newVal;

        return this;
    }

    /**
     * @return Sql escape all.
     */
    public boolean isSqlEscapeAll() {
        return sqlEscapeAll;
    }

    /**
     * @param newVal Sql escape all.
     */
    public CacheClientConfiguration setSqlEscapeAll(boolean newVal) {
        sqlEscapeAll = newVal;

        return this;
    }

    /**
     * @return Sql index max inline size.
     */
    public int getSqlIndexMaxInlineSize() {
        return sqlIdxMaxInlineSize;
    }

    /**
     * @param newVal Sql index max inline size.
     */
    public CacheClientConfiguration setSqlIndexMaxInlineSize(int newVal) {
        sqlIdxMaxInlineSize = newVal;

        return this;
    }

    /**
     * @return Sql schema.
     */
    public String getSqlSchema() {
        return sqlSchema;
    }

    /**
     * @param newVal Sql schema.
     */
    public CacheClientConfiguration setSqlSchema(String newVal) {
        sqlSchema = newVal;

        return this;
    }


    /**
     * @return Cache key configuration.
     */
    public Collection<CacheKeyConfiguration> getKeyConfiguration() {
        return keyCfg;
    }

    /**
     * @param newVal Cache key configuration.
     */
    public CacheClientConfiguration setKeyConfiguration(Collection<CacheKeyConfiguration> newVal) {
        this.keyCfg = new ArrayList<>(newVal);

        return this;
    }

    /**
     * @return Query entities configurations.
     */
    public Collection<QueryEntity> getQueryEntities() {
        return qryEntities;
    }

    /**
     * @param newVal Query entities configurations.
     */
    public CacheClientConfiguration setQueryEntities(Collection<QueryEntity> newVal) {
        qryEntities = new ArrayList<>(newVal);

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof CacheClientConfiguration))
            return false;

        CacheClientConfiguration other = (CacheClientConfiguration)obj;

        return Objects.equals(name, other.name) &&
            atomicityMode == other.atomicityMode &&
            backups == other.backups &&
            cacheMode == other.cacheMode &&
            eagerTtl == other.eagerTtl &&
            Objects.equals(grpName, other.grpName) &&
            dfltLockTimeout == other.dfltLockTimeout &&
            partLossPlc == other.partLossPlc &&
            readFromBackup == other.readFromBackup &&
            rebalanceBatchSize == other.rebalanceBatchSize &&
            rebalanceBatchesPrefetchCnt == other.rebalanceBatchesPrefetchCnt &&
            rebalanceDelay == other.rebalanceDelay &&
            rebalanceMode == other.rebalanceMode &&
            rebalanceOrder == other.rebalanceOrder &&
            rebalanceThrottle == other.rebalanceThrottle &&
            writeSynchronizationMode == other.writeSynchronizationMode &&
            cpOnRead == other.cpOnRead &&
            Objects.equals(dataRegionName, other.dataRegionName) &&
            statisticsEnabled == other.statisticsEnabled &&
            maxConcurrentAsyncOperations == other.maxConcurrentAsyncOperations &&
            maxQryIteratorsCnt == other.maxQryIteratorsCnt &&
            onheapCacheEnabled == other.onheapCacheEnabled &&
            qryDetailMetricsSize == other.qryDetailMetricsSize &&
            qryParallelism == other.qryParallelism &&
            sqlEscapeAll == other.sqlEscapeAll &&
            sqlIdxMaxInlineSize == other.sqlIdxMaxInlineSize &&
            Objects.equals(sqlSchema, other.sqlSchema) &&
            Objects.equals(keyCfg, other.keyCfg) &&
            Objects.equals(qryEntities, other.qryEntities);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 11;

        if (name != null)
            res = 31 * res + name.hashCode();

        if (grpName != null)
            res = 31 * res + grpName.hashCode();

        if (dataRegionName != null)
            res = 31 * res + dataRegionName.hashCode();

        if (sqlSchema != null)
            res = 31 * res + sqlSchema.hashCode();

        res = 31 * res + atomicityMode.ordinal();
        res = 31 * res + backups;
        res = 31 * res + cacheMode.ordinal();
        res = 31 * res + (eagerTtl ? 1 : 0);
        res = 31 * res + (int)(dfltLockTimeout ^ (dfltLockTimeout >>> 32));
        res = 31 * res + partLossPlc.ordinal();
        res = 31 * res + (readFromBackup ? 1 : 0);
        res = 31 * res + rebalanceBatchSize;
        res = 31 * res + (int)(rebalanceBatchesPrefetchCnt ^ (rebalanceBatchesPrefetchCnt >>> 32));
        res = 31 * res + (int)(rebalanceDelay ^ (rebalanceDelay >>> 32));
        res = 31 * res + rebalanceMode.ordinal();
        res = 31 * res + rebalanceOrder;
        res = 31 * res + (int)(rebalanceThrottle ^ (rebalanceThrottle >>> 32));
        res = 31 * res + (int)(rebalanceTimeout ^ (rebalanceTimeout >>> 32));
        res = 31 * res + writeSynchronizationMode.ordinal();
        res = 31 * res + (cpOnRead ? 1 : 0);
        res = 31 * res + (statisticsEnabled ? 1 : 0);
        res = 31 * res + (onheapCacheEnabled ? 1 : 0);
        res = 31 * res + (sqlEscapeAll ? 1 : 0);
        res = 31 * res + maxConcurrentAsyncOperations;
        res = 31 * res + maxQryIteratorsCnt;
        res = 31 * res + qryDetailMetricsSize;
        res = 31 * res + qryParallelism;
        res = 31 * res + sqlIdxMaxInlineSize;
        res = 31 * res + keyCfg.hashCode();
        res = 31 * res + qryEntities.hashCode();

        return res;
    }
}

package org.apache.ignite.client;

import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.configuration.ClientConfiguration;

/** */
public final class Comparers {
    /** Cannot instantiate. */
    private Comparers() {}

    /** */
    public static boolean equal(ClientConfiguration a, Object o) {
        if (!(o instanceof ClientConfiguration))
            return false;

        ClientConfiguration b = (ClientConfiguration)o;

        return Arrays.equals(a.getAddresses(), b.getAddresses()) &&
            a.isTcpNoDelay() == b.isTcpNoDelay() &&
            a.getTimeout() == b.getTimeout() &&
            a.getSendBufferSize() == b.getSendBufferSize() &&
            a.getReceiveBufferSize() == b.getReceiveBufferSize();
    }

    /**  */
    public static boolean equal(ClientCacheConfiguration a, Object o) {
        if (!(o instanceof ClientCacheConfiguration))
            return false;

        ClientCacheConfiguration b = (ClientCacheConfiguration)o;

        return Objects.equals(a.getName(), b.getName()) &&
            a.getAtomicityMode() == b.getAtomicityMode() &&
            a.getBackups() == b.getBackups() &&
            a.getCacheMode() == b.getCacheMode() &&
            a.isEagerTtl() == b.isEagerTtl() &&
            Objects.equals(a.getGroupName(), b.getGroupName()) &&
            a.getDefaultLockTimeout() == b.getDefaultLockTimeout() &&
            a.getPartitionLossPolicy() == b.getPartitionLossPolicy() &&
            a.isReadFromBackup() == b.isReadFromBackup() &&
            a.getRebalanceBatchSize() == b.getRebalanceBatchSize() &&
            a.getRebalanceBatchesPrefetchCount() == b.getRebalanceBatchesPrefetchCount() &&
            a.getRebalanceDelay() == b.getRebalanceDelay() &&
            a.getRebalanceMode() == b.getRebalanceMode() &&
            a.getRebalanceOrder() == b.getRebalanceOrder() &&
            a.getRebalanceThrottle() == b.getRebalanceThrottle() &&
            a.getWriteSynchronizationMode() == b.getWriteSynchronizationMode() &&
            a.isCopyOnRead() == b.isCopyOnRead() &&
            Objects.equals(a.getDataRegionName(), b.getDataRegionName()) &&
            a.isStatisticsEnabled() == b.isStatisticsEnabled() &&
            a.getMaxConcurrentAsyncOperations() == b.getMaxConcurrentAsyncOperations() &&
            a.getMaxQueryIteratorsCount() == b.getMaxQueryIteratorsCount() &&
            a.isOnheapCacheEnabled() == b.isOnheapCacheEnabled() &&
            a.getQueryDetailMetricsSize() == b.getQueryDetailMetricsSize() &&
            a.getQueryParallelism() == b.getQueryParallelism() &&
            a.isSqlEscapeAll() == b.isSqlEscapeAll() &&
            a.getSqlIndexMaxInlineSize() == b.getSqlIndexMaxInlineSize() &&
            Objects.equals(a.getSqlSchema(), b.getSqlSchema()) &&
            Arrays.equals(a.getKeyConfiguration(), b.getKeyConfiguration()) &&
            Arrays.equals(a.getQueryEntities(), b.getQueryEntities());
    }
}

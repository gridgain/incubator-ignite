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

package org.apache.ignite.internal.processors.cache.index;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheCompoundFuture;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheFuture;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheStat;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexOperationCancellationException;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexOperationCancellationToken;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValueHierarchy;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Class for checking the correct completion/stop of index rebuilding.
 */
public class StopRebuildIndexTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteH2IndexingEx.cacheRowConsumer.clear();
        IgniteH2IndexingEx.cacheRebuildRunner.clear();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        IgniteH2IndexingEx.cacheRowConsumer.clear();
        IgniteH2IndexingEx.cacheRebuildRunner.clear();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            ).setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, Person.class)
            );
    }

    /**
     * Checks the correctness {@link SchemaIndexCacheCompoundFuture}.
     */
    @Test
    public void testSchemaIndexCacheCompoundFeature() {
        SchemaIndexCacheCompoundFuture compoundFut = new SchemaIndexCacheCompoundFuture();
        assertFalse(compoundFut.isDone());

        GridFutureAdapter<SchemaIndexCacheStat> fut0 = new GridFutureAdapter<>();
        GridFutureAdapter<SchemaIndexCacheStat> fut1 = new GridFutureAdapter<>();
        GridFutureAdapter<SchemaIndexCacheStat> fut2 = new GridFutureAdapter<>();
        GridFutureAdapter<SchemaIndexCacheStat> fut3 = new GridFutureAdapter<>();

        compoundFut.add(fut0).add(fut1).add(fut2).add(fut3);
        assertFalse(compoundFut.isDone());

        fut0.onDone();
        assertFalse(compoundFut.isDone());

        fut1.onDone();
        assertFalse(compoundFut.isDone());

        fut2.onDone();
        assertFalse(compoundFut.isDone());

        fut3.onDone();
        assertFalse(compoundFut.isDone());

        compoundFut.markInitialized();
        assertTrue(compoundFut.isDone());
        assertNull(compoundFut.error());

        compoundFut = new SchemaIndexCacheCompoundFuture();
        fut0 = new GridFutureAdapter<>();
        fut1 = new GridFutureAdapter<>();
        fut2 = new GridFutureAdapter<>();
        fut3 = new GridFutureAdapter<>();

        compoundFut.add(fut0).add(fut1).add(fut2).add(fut3).markInitialized();
        assertFalse(compoundFut.isDone());

        fut0.onDone();
        assertFalse(compoundFut.isDone());

        Exception err0 = new Exception();
        Exception err1 = new Exception();

        fut1.onDone(err0);
        assertFalse(compoundFut.isDone());

        fut2.onDone(err1);
        assertFalse(compoundFut.isDone());

        fut3.onDone(err1);
        assertTrue(compoundFut.isDone());
        assertEquals(err0, compoundFut.error().getCause());
    }

    /**
     * Checking that when the cluster is deactivated, index rebuilding will be completed correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopRebuildIndexesOnDeactivation() throws Exception {
        stopRebuildIndexes(n -> n.cluster().state(INACTIVE), true);

        assertEquals(1, G.allGrids().size());
    }

    /**
     * Checking that when the node stopped, index rebuilding will be completed correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopRebuildIndexesOnStopNode() throws Exception {
        stopRebuildIndexes(n -> stopAllGrids(), false);
    }

    /**
     * Checking the correctness of the {@code IgniteH2Indexing#idxRebuildFuts}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInternalIndexingRebuildFuture() throws Exception {
        GridQueryProcessor.idxCls = IgniteH2IndexingEx.class;

        IgniteEx n = prepareCluster(10);

        CountDownLatch l0 = new CountDownLatch(1);
        CountDownLatch l1 = new CountDownLatch(1);

        GridCacheContext<?, ?> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        IgniteH2IndexingEx.cacheRebuildRunner.put(
            DEFAULT_CACHE_NAME, () -> assertNull(internalIndexRebuildFuture(n, cacheCtx.cacheId())));

        IgniteH2IndexingEx.cacheRowConsumer.put(DEFAULT_CACHE_NAME, row -> {
            l0.countDown();

            try {
                l1.await(getTestTimeout(), TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                throw new IgniteCheckedException(e);
            }
        });

        n.context().cache().context().database().forceRebuildIndexes(F.asList(cacheCtx));

        IgniteInternalFuture<?> rebFut0 = n.context().query().indexRebuildFuture(cacheCtx.cacheId());
        assertNotNull(rebFut0);

        SchemaIndexCacheFuture rebFut1 = internalIndexRebuildFuture(n, cacheCtx.cacheId());
        assertNotNull(rebFut1);

        assertTrue(l0.await(getTestTimeout(), TimeUnit.MILLISECONDS));
        assertFalse(rebFut0.isDone());

        assertFalse(rebFut1.isDone());
        assertFalse(rebFut1.cancelToken().isCancelled());

        l1.countDown();

        rebFut0.get(getTestTimeout());
        rebFut1.get(getTestTimeout());

        assertFalse(rebFut1.cancelToken().isCancelled());

        assertNull(n.context().query().indexRebuildFuture(cacheCtx.cacheId()));
        assertNull(internalIndexRebuildFuture(n, cacheCtx.cacheId()));
    }

    /**
     * Restart the rebuild of the indexes, checking that it completes gracefully.
     *
     * @param stopRebuildIndexes Stop index rebuild function.
     * @throws Exception If failed.
     */
    private void stopRebuildIndexes(
        IgniteThrowableConsumer<IgniteEx> stopRebuildIndexes,
        boolean expThrowEx
    ) throws Exception {
        GridQueryProcessor.idxCls = IgniteH2IndexingEx.class;

        int keys = 100_000;
        IgniteEx n = prepareCluster(keys);

        IgniteH2IndexingEx.cacheRowConsumer.put(DEFAULT_CACHE_NAME, row -> {
            U.sleep(10);
        });

        GridCacheContext<?, ?> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();
        n.context().cache().context().database().forceRebuildIndexes(F.asList(cacheCtx));

        IgniteInternalFuture<?> fut0 = n.context().query().indexRebuildFuture(cacheCtx.cacheId());
        assertNotNull(fut0);

        SchemaIndexCacheFuture fut1 = internalIndexRebuildFuture(n, cacheCtx.cacheId());
        assertNotNull(fut1);

        CacheMetricsImpl metrics0 = cacheCtx.cache().metrics0();
        assertTrue(metrics0.isIndexRebuildInProgress());
        assertFalse(fut0.isDone());

        assertFalse(fut1.isDone());
        assertFalse(fut1.cancelToken().isCancelled());

        assertTrue(waitForCondition(() -> metrics0.getIndexRebuildKeysProcessed() >= keys / 100, getTestTimeout()));
        assertTrue(metrics0.isIndexRebuildInProgress());
        assertFalse(fut0.isDone());

        assertFalse(fut1.isDone());
        assertFalse(fut1.cancelToken().isCancelled());

        stopRebuildIndexes.accept(n);

        assertFalse(metrics0.isIndexRebuildInProgress());
        assertTrue(metrics0.getIndexRebuildKeysProcessed() < keys);

        if (expThrowEx) {
            assertThrows(log, () -> fut0.get(getTestTimeout()), SchemaIndexOperationCancellationException.class, null);
            assertThrows(log, () -> fut1.get(getTestTimeout()), SchemaIndexOperationCancellationException.class, null);

            assertTrue(fut1.cancelToken().isCancelled());
        }
        else {
            fut0.get(getTestTimeout());

            fut1.get(getTestTimeout());
            assertFalse(fut1.cancelToken().isCancelled());
        }

        assertNull(internalIndexRebuildFuture(n, cacheCtx.cacheId()));
    }

    /**
     * Prepare cluster for test.
     *
     * @param keys Key count.
     * @throws Exception If failed.
     */
    private IgniteEx prepareCluster(int keys) throws Exception {
        IgniteEx n = startGrid(0);

        n.cluster().state(ACTIVE);
        awaitPartitionMapExchange();

        for (int i = 0; i < keys; i++)
            n.cache(DEFAULT_CACHE_NAME).put(i, new Person(i, "p_" + i));

        return n;
    }

    /**
     * Getting internal rebuild index future for the cache.
     *
     * @param n Node.
     * @param cacheId Cache id.
     * @return Rebuild index future.
     */
    @Nullable private SchemaIndexCacheFuture internalIndexRebuildFuture(IgniteEx n, int cacheId) {
        GridQueryIndexing indexing = n.context().query().getIndexing();

        return ((Map<Integer, SchemaIndexCacheFuture>)getFieldValueHierarchy(indexing, "idxRebuildFuts")).get(cacheId);
    }

    /**
     * Extension {@link IgniteH2Indexing} for the test.
     */
    private static class IgniteH2IndexingEx extends IgniteH2Indexing {
        /** Consumer for cache rows when rebuilding indexes. */
        private static Map<String, IgniteThrowableConsumer<CacheDataRow>> cacheRowConsumer = new ConcurrentHashMap<>();

        /** A function that should run before preparing to rebuild the cache indexes. */
        private static Map<String, Runnable> cacheRebuildRunner = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override protected void rebuildIndexesFromHash0(
            GridCacheContext cctx,
            SchemaIndexCacheVisitorClosure clo,
            GridFutureAdapter<Void> rebuildIdxFut,
            SchemaIndexOperationCancellationToken cancel
        ) {
            super.rebuildIndexesFromHash0(cctx, new SchemaIndexCacheVisitorClosure() {
                /** {@inheritDoc} */
                @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
                    cacheRowConsumer.getOrDefault(cctx.name(), r -> {}).accept(row);

                    clo.apply(row);
                }
            }, rebuildIdxFut, cancel);
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> rebuildIndexesFromHash(GridCacheContext cctx) {
            cacheRebuildRunner.getOrDefault(cctx.name(), () -> {}).run();

            return super.rebuildIndexesFromHash(cctx);
        }
    }
}

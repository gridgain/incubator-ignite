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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Iterator;
import java.util.List;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.CacheQueryEntryEvent;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jsr166.ConcurrentLinkedHashMap;
import org.junit.Test;

/**
 * Tests if updates using new counter implementation is applied in expected order.
 */
public class TxPartitionCounterStateUpdatesOrderTest extends TxPartitionCounterStateAbstractTest {
    /** */
    public static final int PARTITION_ID = 0;

    /**
     * Should observe same order of updates on all owners.
     * @throws Exception
     */
    @Test
    public void testSingleThreadedUpdateOrder() throws Exception {
        backups = 2;

        Ignite crd = startGridsMultiThreaded(3);

        IgniteEx client = startGrid("client");

        IgniteCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        List<Integer> keys = partitionKeys(cache, PARTITION_ID, 100, 0);

        cache.put(keys.get(0), new TestVal(keys.get(0)));
        cache.put(keys.get(1), new TestVal(keys.get(1)));
        cache.put(keys.get(2), new TestVal(keys.get(2)));

        assertCountersSame(PARTITION_ID, false);

        cache.remove(keys.get(2));
        cache.remove(keys.get(1));
        cache.remove(keys.get(0));

        assertCountersSame(PARTITION_ID, false);

        WALIterator iter = walIterator((IgniteEx)crd);

        while(iter.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> tup = iter.next();

            if (tup.get2() instanceof DataRecord)
                log.info("next " + tup.get2());
        }

        System.out.println();
    }

    /**
     * TODO same test with historical rebalanbce and different backups(1,2).
     */
    @Test
    public void testSingleThreadedUpdateOrderWithPrimaryRestart() throws Exception {
        backups = 2;

        Ignite crd = startGridsMultiThreaded(3);

        IgniteEx client = startGrid("client");

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        ConcurrentLinkedHashMap<Object, T2<Object, Long>> events = new ConcurrentLinkedHashMap<>();

        qry.setLocalListener(evts -> {
            for (CacheEntryEvent<?, ?> event : evts) {
                CacheQueryEntryEvent e0 = (CacheQueryEntryEvent)event;

                events.put(event.getKey(), new T2<>(event.getValue(), e0.getPartitionUpdateCounter()));
            }
        });

        QueryCursor<Cache.Entry<Object, Object>> cur = client.cache(DEFAULT_CACHE_NAME).query(qry);

        IgniteCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        Ignite prim = G.ignite(client.affinity(DEFAULT_CACHE_NAME).mapPartitionToNode(PARTITION_ID).id());

        Iterator<Integer> it = partitionKeysIterator(cache, PARTITION_ID);

        long stop = U.currentTimeMillis() + 3_000;

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            //while(U.currentTimeMillis() < stop) {
                doSleep(1000);

                stopGrid(prim.name());

                try {
                    awaitPartitionMapExchange();
                    //startGrid(prim.name());
                }
                catch (Exception e) {
                    fail();
                }
            //}
        }, 1, "node-restarter");

        int cnt = 0;

        while(U.currentTimeMillis() < stop) {
            Integer key = it.next();

            cache.put(key, 0);

            cnt++;
        }

        fut.get();

        // Wait until primary rebalance.
        awaitPartitionMapExchange();

        int size = cache.size();

        assertEquals(cnt, size);

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, false);

        cur.close();

        assertEquals(size, events.size());
    }

    private WALIterator walIterator(IgniteEx ignite) throws IgniteCheckedException {
        IgniteWriteAheadLogManager walMgr = ignite.context().cache().context().wal();

        return walMgr.replay(null);
    }

    /** */
    private static class TestVal {
        /** */
        int id;

        /**
         * @param id Id.
         */
        public TestVal(int id) {
            this.id = id;
        }
    }
}

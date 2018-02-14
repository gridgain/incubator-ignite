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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for {@link LocalPendingTransactionsTracker}
 */
public class LocalPendingTransactionsTrackerTest {
    /** Timeout executor. */
    private static ScheduledExecutorService timeoutExecutor;

    /** Tracker. */
    private LocalPendingTransactionsTracker tracker;

    /**
     *
     */
    @BeforeClass
    public static void setUpClass() {
        timeoutExecutor = new ScheduledThreadPoolExecutor(1);
    }

    /**
     *
     */
    @AfterClass
    public static void tearDownClass() {
        timeoutExecutor.shutdown();
    }

    /**
     *
     */
    @Before
    public void setUp() {
        GridTimeoutProcessor time = Mockito.mock(GridTimeoutProcessor.class);
        Mockito.when(time.addTimeoutObject(Mockito.any())).thenAnswer(mock -> {
            GridTimeoutObject timeoutObj = (GridTimeoutObject)mock.getArguments()[0];

            long endTime = timeoutObj.endTime();

            timeoutExecutor.schedule(timeoutObj::onTimeout, endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);

            return null;
        });

        GridCacheSharedContext<?, ?> cctx = Mockito.mock(GridCacheSharedContext.class);
        Mockito.when(cctx.time()).thenReturn(time);

        tracker = new LocalPendingTransactionsTracker(cctx);
    }

    /**
     *
     */
    @Test
    public void testCurrentlyPreparedTxs() {
        txPrepare(1);
        txKeyWrite(1, 10);
        txKeyWrite(1, 11);

        txPrepare(2);
        txKeyWrite(2, 20);
        txKeyWrite(2, 21);
        txKeyWrite(2, 22);

        txPrepare(3);
        txKeyWrite(3, 30);

        txCommit(2);

        tracker.writeLockState();

        try {
            Map<GridCacheVersion, WALPointer> currentlyPreparedTxs = tracker.currentlyPreparedTxs();

            assertEquals(2, currentlyPreparedTxs.size());
            assertTrue(currentlyPreparedTxs.containsKey(nearXidVersion(1)));
            assertTrue(currentlyPreparedTxs.containsKey(nearXidVersion(3)));
        }
        finally {
            tracker.writeUnlockState();
        }

        txKeyWrite(3, 31);
        txCommit(3);

        tracker.writeLockState();

        try {
            Map<GridCacheVersion, WALPointer> currentlyPreparedTxs = tracker.currentlyPreparedTxs();

            assertEquals(1, currentlyPreparedTxs.size());
            assertTrue(currentlyPreparedTxs.containsKey(nearXidVersion(1)));
        }
        finally {
            tracker.writeUnlockState();
        }
    }

    /**
     *
     */
    @Test
    public void testMultiplePrepareCommitMarkers() {
        txPrepare(1);
        txKeyWrite(1, 10);

        txPrepare(2);
        txKeyWrite(2, 20);
        txPrepare(2);
        txKeyWrite(2, 21);
        txPrepare(2);
        txKeyWrite(2, 22);

        txPrepare(3);
        txKeyWrite(3, 30);
        txPrepare(3);
        txKeyWrite(3, 31);

        txCommit(3);
        txCommit(3);

        txCommit(1);

        txCommit(2);
        txCommit(2);

        tracker.writeLockState();

        try {
            Map<GridCacheVersion, WALPointer> currentlyPreparedTxs = tracker.currentlyPreparedTxs();

            assertEquals(1, currentlyPreparedTxs.size());
            assertTrue(currentlyPreparedTxs.containsKey(nearXidVersion(2)));
        }
        finally {
            tracker.writeUnlockState();
        }
    }

    /**
     *
     */
    @Test
    public void testCommitsMoreThanPreparesForbidden() {
        txPrepare(1);

        txKeyWrite(1, 10);
        txKeyWrite(1, 11);

        txCommit(1);

        try {
            txCommit(1);

            fail("We should fail if number of commits is more than number of prepares.");
        }
        catch (Throwable t) {
            // Expected.
        }
    }

    /**
     *
     */
    @Test
    public void testRollback() {
        txRollback(1); // Tx can be rolled back before prepare.

        txPrepare(2);
        txKeyWrite(2, 20);

        txPrepare(3);
        txKeyWrite(3, 30);
        txPrepare(3);
        txKeyWrite(3, 31);

        txCommit(3);

        txRollback(2);
        txRollback(3);

        tracker.writeLockState();

        try {
            Map<GridCacheVersion, WALPointer> currentlyPreparedTxs = tracker.currentlyPreparedTxs();

            assertEquals(0, currentlyPreparedTxs.size());
        }
        finally {
            tracker.writeUnlockState();
        }
    }

    /**
     *
     */
    @Test(timeout = 10_000)
    public void testAwaitFinishOfPreparedTxs() throws Exception {
        txPrepare(1);

        txPrepare(2);
        txPrepare(2);

        txPrepare(3);
        txPrepare(3);
        txCommit(3);

        txPrepare(4);
        txCommit(4);

        txPrepare(5);
        txPrepare(5);
        txPrepare(5);
        txCommit(5);

        tracker.writeLockState();

        IgniteInternalFuture<Set<GridCacheVersion>> fut;
        try {
            fut = tracker.awaitFinishOfPreparedTxs(1_000);
        }
        finally {
            tracker.writeUnlockState();
        }

        Thread.sleep(100);

        txCommit(5);
        txCommit(2);
        txCommit(2);

        long curTs = System.currentTimeMillis();

        Set<GridCacheVersion> pendingTxs = fut.get();

        assertTrue("Waiting for awaitFinishOfPreparedTxs future too long", System.currentTimeMillis() - curTs < 1_000);

        assertEquals(3, pendingTxs.size());
        assertTrue(pendingTxs.contains(nearXidVersion(1)));
        assertTrue(pendingTxs.contains(nearXidVersion(3)));
        assertTrue(pendingTxs.contains(nearXidVersion(5)));

        txCommit(1);
        txCommit(3);
        txCommit(5);

        tracker.writeLockState();

        try {
            fut = tracker.awaitFinishOfPreparedTxs(1_000);
        }
        finally {
            tracker.writeUnlockState();
        }

        assertTrue(fut.get().isEmpty());
    }

    /**
     *
     */
    @Test
    public void trackingCommittedTest() {
        txPrepare(1);
        txCommit(1);

        txPrepare(2);

        tracker.writeLockState();
        try {
            tracker.startTrackingCommitted();
        }
        finally {
            tracker.writeUnlockState();
        }

        txCommit(2);

        txPrepare(3);
        txCommit(3);

        txPrepare(4);

        tracker.writeLockState();

        Map<GridCacheVersion, WALPointer> committedTxs;
        try {
            committedTxs = tracker.stopTrackingCommitted();
        }
        finally {
            tracker.writeUnlockState();
        }

        assertEquals(2, committedTxs.size());
        assertTrue(committedTxs.containsKey(nearXidVersion(2)));
        assertTrue(committedTxs.containsKey(nearXidVersion(3)));
    }

    /**
     *
     */
    @Test
    public void trackingPreparedTest() {
        txPrepare(1);
        txCommit(1);

        txPrepare(2);

        tracker.writeLockState();
        try {
            tracker.startTrackingPrepared();
        }
        finally {
            tracker.writeUnlockState();
        }

        txCommit(2);

        txPrepare(3);
        txCommit(3);

        txPrepare(4);

        tracker.writeLockState();

        Map<GridCacheVersion, WALPointer> committedTxs;
        try {
            committedTxs = tracker.stopTrackingPrepared();
        }
        finally {
            tracker.writeUnlockState();
        }

        assertEquals(2, committedTxs.size());
        assertTrue(committedTxs.containsKey(nearXidVersion(3)));
        assertTrue(committedTxs.containsKey(nearXidVersion(4)));
    }

    /**
     * @param txId Test transaction ID.
     */
    private void txPrepare(int txId) {
        tracker.onTxPrepared(nearXidVersion(txId), new FileWALPointer(0, txId * 10, 1));
    }

    /**
     * @param txId Test transaction ID.
     */
    private void txCommit(int txId) {
        tracker.onTxCommitted(nearXidVersion(txId));
    }

    /**
     * @param txId Test transaction ID.
     */
    private void txRollback(int txId) {
        tracker.onTxRolledBack(nearXidVersion(txId));
    }

    /**
     * @param txId Test transaction ID.
     * @param key Key.
     */
    private void txKeyWrite(int txId, int key) {
        KeyCacheObjectImpl keyCacheObj = new KeyCacheObjectImpl(key, ByteBuffer.allocate(4).putInt(key).array(), 1);

        tracker.onKeysWritten(nearXidVersion(txId), Collections.singletonList(keyCacheObj));
    }

    /**
     * @param txId Test transaction ID.
     * @param key Key.
     */
    private void txKeyRead(int txId, int key) {
        KeyCacheObjectImpl keyCacheObj = new KeyCacheObjectImpl(key, ByteBuffer.allocate(4).putInt(key).array(), 1);

        tracker.onKeysRead(nearXidVersion(txId), Collections.singletonList(keyCacheObj));
    }

    /**
     * @param txId Test transaction ID.
     */
    private GridCacheVersion nearXidVersion(int txId) {
        return new GridCacheVersion(0, txId, 0);
    }
}

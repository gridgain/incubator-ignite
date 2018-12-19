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

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteClosure2X;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 */
public class TxPartitionCounterStateTwoPrimaryTwoBackupsApplyCountersOnRecoveryRollbackTest extends TxPartitionCounterStateAbstractTest {
    /** */
    private static final int [] SIZES = new int[] {5, 7};

    /** */
    private static final int TOTAL = IntStream.of(SIZES).sum() + PRELOAD_KEYS_CNT;

    /** */
    private static final int PARTITION_ID = 0;

    /** */
    private static final int BACKUPS = 1;

    /** */
    private static final int NODES_CNT = 3;

    /** */
    @Test
    public void testPrepareCommitReorder() throws Exception {
        doTestPrepareCommitReorder(false);
    }

    /** */
    @Test
    public void testPrepareCommitReorderSkipCheckpoint() throws Exception {
        doTestPrepareCommitReorder(true);
    }

    /**
     * Test scenario:
     *
     * txs prepared in order 0, 1, 2
     * tx[2] committed out of order.
     * tx[0], tx[1] rolled back due to prepare fail.
     *
     * Pass: counters for rolled back txs are incremented on primary and backup nodes.
     *
     * @param skipCheckpoint Skip checkpoint.
     */
    private void doTestPrepareCommitReorder(boolean skipCheckpoint) throws Exception {
        T2<Ignite, List<Ignite>> txTop = runOnPartition(PARTITION_ID, new Supplier<Integer>() {
                @Override public Integer get() {
                    return PARTITION_ID + 1;
                }
            }, BACKUPS, NODES_CNT,
            new IgniteClosure<Map<Integer, T2<Ignite, List<Ignite>>>, TxCallback>() {
                @Override public TxCallback apply(Map<Integer, T2<Ignite, List<Ignite>>> map) {
                    return new TxCallbackAdapter() {
                        /** */
                        private Queue<Integer> prepOrder = new ConcurrentLinkedQueue<Integer>();

                        {
                            prepOrder.add(0);
                            prepOrder.add(1);
                            prepOrder.add(2);
                        }

                        /** */
                        private Map<IgniteUuid, GridFutureAdapter<?>> prepFuts = new ConcurrentHashMap<>();

                        /** {@inheritDoc} */
                        @Override public boolean beforePrimaryPrepare(IgniteEx primary, IgniteUuid nearXidVer,
                            GridFutureAdapter<?> proceedFut) {
//                            if (map.get(PARTITION_ID).get1() == primary) { // Order prepare for part1
//                                runAsync(() -> {
//                                    prepFuts.put(nearXidVer, proceedFut);
//
//                                    // Order prepares.
//                                    if (prepFuts.size() == SIZES.length) {// Wait until all prep requests queued and force prepare order.
//                                        prepFuts.remove(version(prepOrder.poll())).onDone();
//                                    }
//                                });
//
//                                return true;
//                            }
//
//                            return false;
////                            else
                                return order(nearXidVer) != 1; // Delay txs 0 and 1 for part2, allow tx 2 to finish.
                        }

                        /** {@inheritDoc} */
//                        @Override public boolean afterPrimaryPrepare(IgniteEx primary, IgniteInternalTx tx, IgniteUuid nearXidVer,
//                            GridFutureAdapter<?> fut) {
//                            if (map.get(PARTITION_ID).get1() == primary) {
//                                runAsync(() -> {
//                                    log.info("TX: Prepared part1: " + order(nearXidVer));
//
//                                    if (prepOrder.isEmpty()) {
//                                        log.info("TX: All prepared part1");
//
//                                        return;
//                                    }
//
//                                    prepFuts.remove(version(prepOrder.poll())).onDone();
//                                });
//                            }
//
//                            return order(nearXidVer) != 2;
//                        }
                    };
                }
            },
            SIZES);

        //assertEquals(TOTAL + KEYS_IN_SECOND_PARTITION, grid(CLIENT_GRID_NAME).cache(DEFAULT_CACHE_NAME).size());
    }
}

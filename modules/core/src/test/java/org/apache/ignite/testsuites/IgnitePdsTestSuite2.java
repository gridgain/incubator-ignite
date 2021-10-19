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

package org.apache.ignite.testsuites;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheStartStopWithFreqCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsPartitionFilesDestroyTest;
import org.apache.ignite.internal.processors.cache.persistence.LocalWalModeChangeDuringRebalancingSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.LocalWalModeNoChangeDuringRebalanceOnNonNodeAssignTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncWithDedicatedWorkerSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalHistoryReservationsTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgnitePdsTestSuite2 {
    /**
     * @return Suite.
     */
    public static List<Class<?>> suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();
//
//        // Integrity test.
//        GridTestUtils.addTestIfNeeded(suite, IgniteDataIntegrityTests.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteStandaloneWalIteratorInvalidCrcTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteReplayWalIteratorInvalidCrcTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteFsyncReplayWalIteratorInvalidCrcTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgnitePureJavaCrcCompatibility.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteWithoutArchiverWalIteratorInvalidCrcTest.class, ignoredTests);

        addRealPageStoreTests(suite, ignoredTests);
//
//        addRealPageStoreTestsNotForDirectIo(suite, ignoredTests);
//
//        // BaselineTopology tests
//        GridTestUtils.addTestIfNeeded(suite, IgniteAllBaselineNodesOnlineFullApiSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteOfflineBaselineNodeFullApiSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteOnlineNodeOutOfBaselineFullApiSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, ClientAffinityAssignmentWithBaselineTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteAbsentEvictionNodeOutOfBaselineTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, ClusterActivationEventTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, ClusterActivationEventWithPersistenceTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, ClusterStateChangeEventTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, ClusterStateChangeEventWithPersistenceTest.class, ignoredTests);
//
//        // Maintenance tests
//        GridTestUtils.addTestIfNeeded(suite, MaintenanceRegistrySimpleTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, WalArchiveSizeConfigurationTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, CheckpointMarkerReadingErrorOnStartTest.class, ignoredTests);

        return suite;
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store, but requires long time to
     * execute.
     *
     * @param suite suite to add tests into.
     * @param ignoredTests Ignored tests.
     */
    private static void addRealPageStoreTestsNotForDirectIo(List<Class<?>> suite, Collection<Class> ignoredTests) {
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsPartitionFilesDestroyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, LocalWalModeChangeDuringRebalancingSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, LocalWalModeNoChangeDuringRebalanceOnNonNodeAssignTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushFsyncSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushFsyncWithDedicatedWorkerSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushFsyncWithMmapBufferSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCacheStartStopWithFreqCheckpointTest.class, ignoredTests);
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store and does actual disk operations.
     *
     * NOTE: These tests are also executed using I/O plugins.
     *
     * @param suite suite to add tests into.
     * @param ignoredTests Ignored tests.
     */
    public static void addRealPageStoreTests(List<Class<?>> suite, Collection<Class> ignoredTests) {
//        GridTestUtils.addTestIfNeeded(suite, StandaloneWalRecordsIteratorTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, IgnitePdsPageSizesTest.class, ignoredTests);
//
//        // Metrics test.
//        GridTestUtils.addTestIfNeeded(suite, IgniteDataStorageMetricsSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgnitePdsRebalancingOnNotStableTopologyTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgnitePdsWholeClusterRestartTest.class, ignoredTests);

        // Rebalancing test
        GridTestUtils.addTestIfNeeded(suite, IgniteWalHistoryReservationsTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, SlowHistoricalRebalanceSmallHistoryTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteShutdownOnSupplyMessageFailureTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgnitePersistentStoreDataStructuresTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, FullHistRebalanceOnClientStopTest.class, ignoredTests);
//
//        // Failover test
//        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushFailoverTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushBackgroundSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushBackgroundWithMmapBufferSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushLogOnlySelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushLogOnlyWithMmapBufferSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteWalFormatFileFailoverTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgnitePdsNoSpaceLeftOnDeviceTest.class, ignoredTests);
//
//        // Test suite uses Standalone WAL iterator to verify PDS content.
//        GridTestUtils.addTestIfNeeded(suite, IgniteWalReaderTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgnitePdsExchangeDuringCheckpointTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgnitePdsReserveWalSegmentsTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgnitePdsReserveWalSegmentsWithCompactionTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteWalReplayingAfterRestartTest.class, ignoredTests);
//
//        // CDC tests.
//        GridTestUtils.addTestIfNeeded(suite, CdcSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, CdcCacheVersionTest.class, ignoredTests);
//
//        // new style folders with generated consistent ID test
//        GridTestUtils.addTestIfNeeded(suite, IgniteUidAsConsistentIdMigrationTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteWalSerializerVersionTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, WalCompactionTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, WalCompactionNoArchiverTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, WalCompactionSwitchOnTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, WalDeletionArchiveFsyncTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, WalDeletionArchiveLogOnlyTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, IgniteCheckpointDirtyPagesForLowLoadTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCorruptedStoreTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, CheckpointFailBeforeWriteMarkTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, CheckpointFreeListTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, CheckpointListenerForRegionTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, LightweightCheckpointTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, CheckpointStartLoggingTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, FreeListCachingTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteWalIteratorSwitchSegmentTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteWalIteratorExceptionDuringReadTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteNodeStoppedDuringDisableWALTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, FilteredWalIteratorTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, WalScannerTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteWalRecoverySeveralRestartsTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteRebalanceScheduleResendPartitionsTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteWALTailIsReachedDuringIterationOverArchiveTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, WalRolloverTypesTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, FsyncWalRolloverDoesNotBlockTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgnitePdsPartitionsStateRecoveryTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, WalPreloadingConcurrentTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, WALPreloadingWithCompactionTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, IgniteWalRebalanceLoggingTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, HistoricalRebalanceHeuristicsTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, IgniteLocalWalSizeTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, WriteAheadLogManagerSelfTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, HistoricalRebalanceTwoPartsInDifferentCheckpointsTest.class, ignoredTests);
    }
}

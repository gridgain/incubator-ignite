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

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.mvcc.*;
import org.apache.ignite.internal.processors.datastreamer.CacheMvccDataStreamProcessorSeflTest;

/**
 *
 */
public class IgniteCacheMvccTestSuite extends TestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("IgniteCache MVCC Test Suite");

        suite.addTestSuite(CacheMvccTransactionsTest.class);
        suite.addTestSuite(CacheMvccProcessorTest.class);
        suite.addTestSuite(CacheMvccClusterRestartTest.class);
        suite.addTestSuite(CacheMvccConfigurationValidationTest.class);
        suite.addTestSuite(CacheMvccOperationChecksTest.class);
        suite.addTestSuite(CacheMvccIteratorWithConcurrentTransactionTest.class);
        suite.addTestSuite(CacheMvccLocalEntriesWithConcurrentTransactionTest.class);
        suite.addTestSuite(CacheMvccScanQueryWithConcurrentTransactionTest.class);
        suite.addTestSuite(CacheMvccSizeWithConcurrentTransactionTest.class);
        suite.addTestSuite(CacheMvccVacuumTest.class);
        suite.addTestSuite(CacheMvccPartitionedCoordinatorFailoverTest.class);
        suite.addTestSuite(CacheMvccReplicatedCoordinatorFailoverTest.class);
        suite.addTestSuite(CacheMvccDataStreamProcessorSeflTest.class);

        return suite;
    }
}

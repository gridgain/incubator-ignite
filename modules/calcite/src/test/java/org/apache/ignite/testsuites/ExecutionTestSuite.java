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

import org.apache.ignite.internal.processors.query.calcite.exec.rel.ContinuousExecutionTest;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ExecutionTest;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.MergeJoinExecutionTest;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.NestedLoopJoinExecutionTest;
import org.apache.ignite.internal.processors.query.calcite.planner.IndexSpoolTest;
import org.apache.ignite.internal.processors.query.calcite.planner.TableSpoolTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Calcite tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ExecutionTest.class,
    ContinuousExecutionTest.class,
    MergeJoinExecutionTest.class,
    NestedLoopJoinExecutionTest.class,
    TableSpoolTest.class,
    IndexSpoolTest.class,
})
public class ExecutionTestSuite {
}

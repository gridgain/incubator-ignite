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


package org.apache.ignite.custom;

import org.apache.ignite.configuration.internal.Selectors;
import org.apache.ignite.configuration.internal.property.DynamicProperty;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class DynConfigTest extends GridCommonAbstractTest {

    @Test public void test() throws Exception {
        final IgniteEx ex0 = startGrid(0);
        final IgniteEx ex1 = startGrid(1);

        final DynamicProperty<Boolean> internal = ex0.clusterWideConfiguration().getInternal(Selectors.CLUSTER_BASELINE_AUTO_ADJUST_ENABLED_REC);

        internal.change(true);

        GridTestUtils.waitForCondition(() -> {
            return ex1.context().state().isBaselineAutoAdjustEnabled();
        }, 10_000);

        internal.change(false);

        GridTestUtils.waitForCondition(() -> {
            return !ex1.context().state().isBaselineAutoAdjustEnabled();
        }, 10_000);
    }

}

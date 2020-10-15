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

package org.apache.ignite.configuration.internal;

import java.util.Collections;
import org.apache.ignite.Selectors;
import org.apache.ignite.configuration.internal.property.DynamicProperty;
import org.apache.ignite.configuration.internal.property.NamedList;
import org.junit.Test;

public class UsageTest {

    @Test
    public void test() {
        final InitNode node = new InitNode();
        node.withPort(1000);
        node.withConsistentId("1000");
        final NamedList<InitNode> namedListInitNode = new NamedList<>(Collections.singletonMap("node1", node));
        LocalConfiguration localConfiguration = new LocalConfiguration();
        final InitBaseline baseline = new InitBaseline();
        baseline.withNodes(namedListInitNode);
        final InitLocal local = new InitLocal();
        local.withBaseline(baseline);
        localConfiguration.init(local);
        localConfiguration.baseline().autoAdjust().enabled(false);
        final DynamicProperty<String> node1ViaFn = Selectors.LOCAL_BASELINE_NODES_CONSISTENT_ID_FN("node1").select(localConfiguration);
        final DynamicProperty<String> node1ViaFluentAPI = localConfiguration.baseline().nodes().get("node1").consistentId();
    }

}

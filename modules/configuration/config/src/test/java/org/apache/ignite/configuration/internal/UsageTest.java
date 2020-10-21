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

import java.io.Serializable;
import java.util.Collections;
import java.util.function.Consumer;
import org.apache.ignite.configuration.internal.property.DynamicProperty;
import org.apache.ignite.configuration.internal.property.NamedList;
import org.apache.ignite.configuration.internal.selector.Selector;
import org.junit.Test;

public class UsageTest {

    @Test
    public void test() {
        LocalConfiguration localConfiguration = new LocalConfiguration();
        final ConfigurationStorage storage = new ConfigurationStorage() {

            @Override
            public <T extends Serializable> void save(String propertyName, T object) {

            }

            @Override
            public <T extends Serializable> T get(String propertyName) {
                return null;
            }

            @Override
            public <T extends Serializable> void listen(String key, Consumer<T> listener) {

            }
        };

        final Configurator<LocalConfiguration> configurator = new Configurator<>(storage, localConfiguration);

        InitLocal initLocal = new InitLocal().withBaseline(
            new InitBaseline()
                .withNodes(
                    new NamedList<>(
                        Collections.singletonMap("node1", new InitNode().withConsistentId("test").withPort(1000))
                    )
                )
        );

        configurator.set(Selectors.LOCAL_BASELINE_NODES_FN("node1"), new ChangeNode().withPort(1000));

        localConfiguration.init(initLocal);

        final DynamicProperty<String> node1 = configurator.getInternal(Selectors.LOCAL_BASELINE_NODES_CONSISTENT_ID_FN("node1"));

        localConfiguration.baseline().autoAdjust().enabled(false);
        final DynamicProperty<String> node1ViaFn = Selectors.LOCAL_BASELINE_NODES_CONSISTENT_ID_FN("node1").select(localConfiguration);
        final DynamicProperty<String> node1ViaFluentAPI = localConfiguration.baseline().nodes().get("node1").consistentId();

        final Selector selector = Selectors.find("local.baseline.nodes[node1].port");
        final DynamicProperty<Integer> portViaSel = (DynamicProperty<Integer>) selector.select(localConfiguration);
    }

}

/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.configuration.internal;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.Serializable;
import java.io.StringReader;
import java.util.function.Consumer;
import org.apache.ignite.configuration.internal.selector.Selector;

public class CLIExample {

    public static void main(String[] args) {
        final String arg = args[0];

        final Config config = ConfigFactory.parseReader(new StringReader(arg));
        config.resolve();

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

        Selectors.CLUSTER_BASELINE_NODES_CONSISTENT_ID_FN("");

        final Configurator<LocalConfiguration> configurator = new Configurator<>(storage, localConfiguration);

        config.entrySet().forEach(entry -> {
            final String key = entry.getKey();
            final Object value = entry.getValue().unwrapped();
            final Selector selector = Selectors.find(key);
            if (selector != null)
                configurator.set(selector, value);
        });
    }

}

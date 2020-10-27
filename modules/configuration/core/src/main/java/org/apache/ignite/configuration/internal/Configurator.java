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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.ignite.configuration.internal.property.DynamicProperty;
import org.apache.ignite.configuration.internal.property.Modifier;
import org.apache.ignite.configuration.internal.property.PropertyListener;
import org.apache.ignite.configuration.internal.selector.Selector;

public class Configurator<T extends DynamicConfiguration<?, ?, ?>> {

    private final ConfigurationStorage storage;

    private final T root;

    public Configurator(ConfigurationStorage storage, T root) {
        this.storage = storage;
        this.root = root;
        this.init();
    }

    private void init() {
        List<DynamicProperty> props = new ArrayList<>();
        Queue<DynamicConfiguration<?, ?, ?>> confs = new LinkedList<>();
        confs.add(root);

        while (!confs.isEmpty()) {
            final DynamicConfiguration<?, ?, ?> conf = confs.poll();
            conf.members.values().forEach(modifier -> {
                if (modifier instanceof DynamicConfiguration) {
                    confs.add((DynamicConfiguration<?, ?, ?>) modifier);
                    ((DynamicConfiguration<?, ?, ?>) modifier).setConfigurator(this);
                } else {
                    props.add((DynamicProperty<?>) modifier);
                    ((DynamicProperty<?>) modifier).setConfigurator(this);
                }
            });
        }

        props.forEach(property -> {
            final String key = property.key();
            property.addListener(new PropertyListener() {
                @Override public void update(Serializable newValue, Modifier modifier) {
                    storage.save(key, newValue);
                }

                @Override public String id() {
                    return ConfigurationStorage.STORAGE_LISTENER_ID;
                }
            });
            storage.listen(key, serializable -> {
                property.setSilently(serializable);
            });
        });
    }

    public <TARGET extends Modifier<VIEW, INIT, CHANGE>, VIEW, INIT, CHANGE> VIEW getPublic(Selector<T, TARGET, VIEW, INIT, CHANGE> selector) {
        return selector.select(root).toView();
    }

    public <TARGET extends Modifier<VIEW, INIT, CHANGE>, VIEW, INIT, CHANGE> void set(Selector<T, TARGET, VIEW, INIT, CHANGE> selector, CHANGE newValue) {
        final TARGET select = selector.select(root);
        select.change(newValue);
    }

    public <TARGET extends Modifier<VIEW, INIT, CHANGE>, VIEW, INIT, CHANGE> void init(Selector<T, TARGET, VIEW, INIT, CHANGE> selector, INIT initValue) {
        final TARGET select = selector.select(root);
        select.init(initValue);
    }

    public <TARGET extends Modifier<VIEW, INIT, CHANGE>, VIEW, INIT, CHANGE> TARGET getInternal(Selector<T, TARGET, VIEW, INIT, CHANGE> selector) {
        return selector.select(root);
    }

}

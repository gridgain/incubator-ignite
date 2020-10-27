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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.internal.property.Modifier;
import org.apache.ignite.configuration.internal.property.NamedList;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class NamedListConfiguration<U, T extends Modifier<U, INIT, CHANGE>, INIT, CHANGE> extends DynamicConfiguration<NamedList<U>, NamedList<INIT>, NamedList<CHANGE>> {
    /** Creator of named configuration. */
    private final BiFunction<String, String, T> creator;

    /** Named configurations. */
    Map<String, T> values = new HashMap<>();

    public NamedListConfiguration(String prefix, String key, BiFunction<String, String, T> creator) {
        super(prefix, key);
        this.creator = creator;
    }

    /** {@inheritDoc} */
    @Override public void change(NamedList<CHANGE> o) {
        o.getValues().forEach((key, change) -> {
            if (!values.containsKey(key))
                values.put(key, add(creator.apply(qualifiedName, key)));

            values.get(key).change(change);
        });
    }

    /** {@inheritDoc} */
    @Override public void init(NamedList<INIT> o) {
        o.getValues().forEach((key, init) -> {
            if (!values.containsKey(key))
                values.put(key, add(creator.apply(qualifiedName, key)));

            values.get(key).init(init);
        });
    }

    public T get(String name) {
        return values.get(name);
    }

    /** {@inheritDoc} */
    @Override public NamedList<U> toView() {
        return new NamedList<>(values.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, it -> it.getValue().toView())));
    }
}

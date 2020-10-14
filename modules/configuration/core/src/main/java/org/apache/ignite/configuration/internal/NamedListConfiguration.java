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
public class NamedListConfiguration<U, T extends Modifier<U>> extends DynamicConfiguration<NamedList<U>, Object, Object> {
    private final BiFunction<String, String, T> creator;

    Map<String, T> values = new HashMap<>();

    public NamedListConfiguration(String prefix, String key, BiFunction<String, String, T> creator) {
        super(prefix, key);
        this.creator = creator;
    }

    @Override public void change(Object o) {

    }

    @Override public void init(Object o) {

    }

    public T get(String name) {
        return values.get(name);
    }

    @Override public void updateValue(String key, Object newValue) {
        String name = key.split("\\.")[1];
        if (!values.containsKey(name))
            values.put(name, add(creator.apply(qualifiedName, name)));

        super.updateValue(key, newValue);
    }

    @Override public NamedList<U> toView() {
        return new NamedList<>(values.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, it -> it.getValue().toView())));
    }

//    public void accept(String path, ConfigTreeVisitor visitor) {
//        visitor.visit(path, this);
////        timeout.accept(path, visitor);
////        enabled.accept(path, visitor);
//    }
}

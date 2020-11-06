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
import org.apache.ignite.configuration.internal.property.Modifier;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class DynamicConfiguration<T, INIT, CHANGE> implements Modifier<T, INIT, CHANGE> {
    /** Fully qualified name of the configuration. */
    protected final String qualifiedName;

    protected final String key;

    protected final String prefix;

    protected final Map<String, Modifier> members = new HashMap<>();

    protected final DynamicConfiguration<?, ?, ?> root;

    protected DynamicConfiguration(String prefix, String key, DynamicConfiguration<?, ?, ?> root) {
        this.prefix = prefix;
        this.qualifiedName = String.format("%s.%s", prefix, key);
        this.key = key;
        this.root = root != null ? root : this;
    }

    protected <M extends Modifier> M add(M member) {
        members.put(member.key(), member);

        return member;
    }

    private String nextKey(String key) {
        int of = key.indexOf('.');

        return of == -1 ? key : key.substring(0, of);
    }

    private String nextPostfix(String key) {
        String start = key.substring(0, key.indexOf('.'));
        if (!start.equals(this.key))
            throw new IllegalArgumentException();

        key = key.substring(this.key.length() + 1);

        return key;
    }

    @Override public Modifier<T, INIT, CHANGE> find(String key) {
        if (key.equals(this.key))
            return this;

        key = nextPostfix(key);

        return members.get(nextKey(key)).find(key);
    }

    @Override public String key() {
        return key;
    }

    protected abstract DynamicConfiguration<T, INIT, CHANGE> copy(DynamicConfiguration<?, ?, ?> root);

    protected final DynamicConfiguration<T, INIT, CHANGE> copy() {
        return copy(null);
    }

    @Override public void validate() {
        members.values().forEach(Modifier::validate);
    }
}

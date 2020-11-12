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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.configuration.internal.property.DynamicProperty;
import org.apache.ignite.configuration.internal.property.Modifier;
import org.apache.ignite.configuration.internal.selector.BaseSelectors;
import org.apache.ignite.configuration.internal.validation.FieldValidator;

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

    protected final Map<String, Modifier<?, ?, ?>> members = new HashMap<>();

    protected final DynamicConfiguration<?, ?, ?> root;

    protected final boolean isNamed;

    protected final Configurator<? extends DynamicConfiguration<?, ?, ?>> configurator;

    protected DynamicConfiguration(
        String prefix,
        String key,
        boolean isNamed,
        Configurator<? extends DynamicConfiguration<?, ?, ?>> configurator,
        DynamicConfiguration<?, ?, ?> root
    ) {
        this.prefix = prefix;
        this.isNamed = isNamed;
        this.configurator = configurator;

        this.key = key;
        if (root == null)
            this.qualifiedName = key;
        else {
            if (isNamed)
                qualifiedName = String.format("%s[%s]", prefix, key);
            else
                qualifiedName = String.format("%s.%s", prefix, key);
        }

        this.root = root != null ? root : this;
    }

    protected <M extends Modifier<?, ?, ?>> M add(M member) {
        members.put(member.key(), member);

        return member;
    }

    protected <PROP extends Serializable, M extends DynamicProperty<PROP>> M add(M member, List<FieldValidator<? super PROP, ? extends DynamicConfiguration<?, ?, ?>>> validators) {
        members.put(member.key(), member);

        configurator.addValidations((Class<? extends DynamicConfiguration<?, ?, ?>>) getClass(), member.key(), validators);

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

        return (Modifier<T, INIT, CHANGE>) members.get(nextKey(key)).find(key);
    }

    @Override public void init(INIT init) {
        configurator.init(BaseSelectors.find(qualifiedName), init);
    }

    @Override public void change(CHANGE change) {
        configurator.set(BaseSelectors.find(qualifiedName), change);
    }

    @Override public String key() {
        return key;
    }

    protected abstract DynamicConfiguration<T, INIT, CHANGE> copy(DynamicConfiguration<?, ?, ?> root);

    protected final DynamicConfiguration<T, INIT, CHANGE> copy() {
        return copy(null);
    }

    @Override public void validate(DynamicConfiguration<?, ?, ?> oldRoot) {
        members.values().forEach(member -> member.validate(oldRoot));
    }

}

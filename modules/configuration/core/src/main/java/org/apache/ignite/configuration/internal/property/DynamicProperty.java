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

package org.apache.ignite.configuration.internal.property;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.configuration.internal.Configurator;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.internal.selector.BaseSelectors;
import org.apache.ignite.configuration.internal.validation.FieldValidator;
import org.apache.ignite.configuration.internal.validation.MemberKey;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class DynamicProperty<T extends Serializable> implements Modifier<T, T, T> {
    /** Name of property. */
    private final String name;

    /** Member key. */
    private final MemberKey memberKey;

    /** Full name with prefix. */
    private final String qualifiedName;

    /** Property value. */
    protected volatile T val;

    /** Listeners of property update. */
    private final List<PropertyListener<T, T, T>> updateListeners = new ArrayList<>();

    protected final Configurator<? extends DynamicConfiguration<?, ?, ?>> configurator;

    protected final DynamicConfiguration<?, ?, ?> root;

    public DynamicProperty(
        String prefix,
        String name,
        MemberKey memberKey,
        Configurator<? extends DynamicConfiguration<?, ?, ?>> configurator,
        DynamicConfiguration<?, ?, ?> root
    ) {
        this(prefix, name, memberKey, null, configurator, root);
    }

    public DynamicProperty(
        String prefix,
        String name,
        MemberKey memberKey,
        T defaultValue,
        Configurator<? extends DynamicConfiguration<?, ?, ?>> configurator,
        DynamicConfiguration<?, ?, ?> root
    ) {
        this(defaultValue, name, memberKey, String.format("%s.%s", prefix, name), configurator, root);
    }

    private DynamicProperty(
        DynamicProperty<T> base,
        DynamicConfiguration<?, ?, ?> root
    ) {
        this(base.val, base.name, base.memberKey, base.qualifiedName, base.configurator, root);
    }

    private DynamicProperty(
        T value,
        String name,
        MemberKey memberKey,
        String qualifiedName,
        Configurator<? extends DynamicConfiguration<?, ?, ?>> configurator,
        DynamicConfiguration<?, ?, ?> root
    ) {
        this.name = name;
        this.memberKey = memberKey;
        this.qualifiedName = qualifiedName;
        this.val = value;
        this.configurator = configurator;
        this.root = root;
    }

    public boolean addListener(PropertyListener<T, T, T> listener) {
        return updateListeners.add(listener);
    }

    public T value() {
        return val;
    }

    @Override public T toView() {
        return val;
    }

    @Override public Modifier<T, T, T> find(String key) {
        if (key.equals(name))
            return this;

        return null;
    }

    @Override public void change(T object) {
        configurator.set(BaseSelectors.find(qualifiedName), object);
    }

    @Override public void init(T object) {
        configurator.init(BaseSelectors.find(qualifiedName), object);
    }

    @Override public void changeWithoutValidation(T object) {
        this.val = object;
        updateListeners.forEach(listener -> {
            listener.update(object, this);
        });
    }

    @Override public void initWithoutValidation(T object) {
        this.val = object;
    }

    @Override public void validate(DynamicConfiguration<?, ?, ?> oldRoot) {
        final List<FieldValidator<? extends Serializable, ? extends DynamicConfiguration<?, ?, ?>>> validators = configurator.validators(memberKey);
        validators.forEach(v -> ((FieldValidator) v).validate(val, root, oldRoot));
    }

    @Override public String key() {
        return name;
    }

    public String qualifiedName() {
        return qualifiedName;
    }

    public void setSilently(T serializable) {
        val = serializable;
        updateListeners.forEach(listener -> {
            listener.update(val, this);
        });
    }

    public DynamicProperty<T> copy(DynamicConfiguration<?, ?, ?> newRoot) {
        return new DynamicProperty<>(this, newRoot);
    }

}

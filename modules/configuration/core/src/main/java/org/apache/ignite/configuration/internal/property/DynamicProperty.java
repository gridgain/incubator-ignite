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
import org.apache.ignite.configuration.internal.ConfigurationStorage;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.internal.validation.FieldValidator;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class DynamicProperty<T extends Serializable> implements Modifier<T, T, T> {
    /** Name of property. */
    private final String name;

    /** Full name with prefix. */
    private final String qualifiedName;

    /** Property value. */
    protected volatile T val;

    private final List<FieldValidator<? super T, ?>> validators;

    /** Listeners of property update. */
    private final List<PropertyListener<T, T, T>> updateListeners = new ArrayList<>();

    protected DynamicConfiguration<?, ?, ?> root;

    public DynamicProperty(
        String prefix,
        String name,
        List<FieldValidator<? super T, ?>> validators,
        DynamicConfiguration<?, ?, ?> root
    ) {
        this(prefix, name, null, validators, root);
    }

    public DynamicProperty(
        String prefix,
        String name,
        T defaultValue,
        List<FieldValidator<? super T, ?>> validators,
        DynamicConfiguration<?, ?, ?> root
    ) {
        this(defaultValue, name, String.format("%s.%s", prefix, name), validators, root);
    }

    private DynamicProperty(DynamicProperty<T> base, DynamicConfiguration<?, ?, ?> root) {
        this(base.name, base.qualifiedName, base.val, base.validators, root);
    }

    private DynamicProperty(
        T value,
        String name,
        String qualifiedName,
        List<FieldValidator<? super T, ?>> validators,
        DynamicConfiguration<?, ?, ?> root
    ) {
        this.name = name;
        this.qualifiedName = qualifiedName;
        this.val = value;
        this.validators = validators;
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

    @Override public void change(T object, boolean validate) {
        if (validate)
            validate0(object);

        this.val = object;
        updateListeners.forEach(listener -> {
            listener.update(object, this);
        });
    }

    @Override public void init(T object, boolean validate) {
        if (validate)
            validate0(object);

        this.val = object;
    }

    @Override public void validate() {
        validators.forEach(v -> ((FieldValidator) v).validate(val, root));
    }

    private void validate0(T val) {
        validators.forEach(v -> ((FieldValidator) v).validate(val, root));
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
            if (!listener.id().equals(ConfigurationStorage.STORAGE_LISTENER_ID))
                listener.update(val, this);
        });
    }

    public DynamicProperty<T> copy(DynamicConfiguration<?, ?, ?> newRoot) {
        return new DynamicProperty<>(this, newRoot);
    }

}

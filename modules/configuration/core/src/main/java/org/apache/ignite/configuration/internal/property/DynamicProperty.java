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

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class DynamicProperty<T> implements Modifier<T> {
    /** Name of property. */
    private final String name;

    /** Full name with prefix. */
    private final String qualifiedName;

    /** Property value. */
    protected volatile T val;

    /** Listeners of property update. */
    private final ConcurrentLinkedQueue<DynamicProperty<? super T>> updateListeners = new ConcurrentLinkedQueue<>();

    public DynamicProperty(String prefix, String name) {
        this(prefix, name, null);
    }

    public DynamicProperty(String prefix, String name, T defaultValue) {
        this.name = name;
        this.qualifiedName = String.format("%s.%s", prefix, name);
        this.val = defaultValue;
    }

    public T value() {
        return val;
    }

    @Override public T toView() {
        return val;
    }

    @Override public Modifier<T> find(String key) {
        if (key.equals(name))
            return this;

        return null;
    }

    public void change(T object) {
        this.val = object;
    }

    public void init(T object) {
        this.val = object;
    }

    @Override public void updateValue(String key, Object object) {
        if (!name.equals(key))
            throw new IllegalArgumentException();

        val = (T)object;
    }

    @Override public String key() {
        return name;
    }

    public String qualifiedName() {
        return qualifiedName;
    }

    public void accept(String path, ConfigTreeVisitor visitor) {
        visitor.visit(path, this);
    }
}

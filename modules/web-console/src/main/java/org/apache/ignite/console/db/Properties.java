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

package org.apache.ignite.console.db;

import java.util.HashMap;
import java.util.Map;

import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.console.db.PropertyType.ARRAY;
import static org.apache.ignite.console.db.PropertyType.BOOLEAN;
import static org.apache.ignite.console.db.PropertyType.NUMBER;
import static org.apache.ignite.console.db.PropertyType.STRING;
import static org.apache.ignite.console.db.PropertyType.UUID;

/**
 * Class that holds metadata about DTO types.
 */
public class Properties {
    /** */
    private Map<String, Object> props = new HashMap<>();

    /**
     * Add property.
     *
     * @param key Property name.
     * @param type Property type.
     * @return {@code this} for chaining.
     */
    private Properties addProperty(String key, PropertyType type) {
        props.put(key, type);

        return this;
    }

    /**
     * Add array property.
     *
     * @param key Property name.
     * @return {@code this} for chaining.
     */
    public Properties addArray(String key) {
        return addProperty(key, ARRAY);
    }

    /**
     * Add boolean property.
     *
     * @param key Property name.
     * @return {@code this} for chaining.
     */
    public Properties addBoolean(String key) {
        return addProperty(key, BOOLEAN);
    }

    /**
     * Add number property.
     *
     * @param key Property name.
     * @return {@code this} for chaining.
     */
    public Properties addNumber(String key) {
        return addProperty(key, NUMBER);
    }

    /**
     * Add string property.
     *
     * @param key Property name.
     * @return {@code this} for chaining.
     */
    public Properties addString(String key) {
        return addProperty(key, STRING);
    }

    /**
     * Add UUID property.
     *
     * @param key Property name.
     * @return {@code this} for chaining.
     */
    public Properties addUuid(String key) {
        return addProperty(key, UUID);
    }

    /**
     * Add child properties.
     *
     * @param key Property name.
     * @param child Child properties.
     * @return {@code this} for chaining.
     */
    public Properties addChild(String key, Properties child) {
        props.put(key, child);

        return this;
    }

    /**
     * @param key Key to check.
     * @return {@code true} if schema has property.
     */
    public boolean hasProperty(String key) {
        return props.containsKey(key);
    }

    /**
     * Get child schema.
     *
     * @param key Key.
     * @return Child schema.
     */
    @Nullable public Properties childSchema(String key) {
        Object v = props.get(key);

        if (v instanceof Properties)
            return (Properties) v;

        return null;
    }
}

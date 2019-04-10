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
package org.apache.ignite.console.util;

import java.util.LinkedHashMap;
import java.util.UUID;

import static org.apache.ignite.console.util.JsonUtils.toJson;

/**
 * A helper class to build a JSON object in Java.
 */
public class JsonObject extends LinkedHashMap<String, Object> {
    /**
     * @param key Key.
     * @param val Value.
     * @return {@code this} for chaining.
     */
    public JsonObject add(String key, Object val) {
        put(key, val);

        return this;
    }

    /**
     * Get the boolean value with the specified key.
     *
     * @param key Key.
     * @param dflt Default value, if map does not contain key.
     * @return Value or {@code dflt} if no value for that key.
     * @throws ClassCastException If the value is not a {@link Boolean}.
     */
    public Boolean getBoolean(String key, boolean dflt) {
        Boolean val = (Boolean)get(key);

        return val != null || containsKey(key) ? val : dflt;
    }

    /**
     * Get the string value with the specified key.
     *
     * @param key Key.
     * @return Value or {@code null} if no value for that key.
     * @throws ClassCastException If the value is not a {@link String}.
     */
    public String getString(String key) {
        return (String)get(key);
    }

    /**
     * Get the string value with the specified key.
     *
     * @param key Key.
     * @param dflt Default value, if map does not contain key.
     * @return Value or {@code dflt} if no value for that key.
     * @throws ClassCastException If the value is not a {@link String}.
     */
    public String getString(String key, String dflt) {
        String val = (String)get(key);

        return val != null || containsKey(key) ? val : dflt;
    }

    /**
     * Get the integer value with the specified key.
     *
     * @param key Key.
     * @return Value or {@code null} if no value for that key.
     * @throws ClassCastException If the value is not a {@link Integer}.
     */
    public Integer getInteger(String key) {
        return (Integer)get(key);
    }

    /**
     * Get the integer value with the specified key.
     *
     * @param key Key.
     * @param dflt Default value, if map does not contain key.
     * @return Value or {@code dflt} if no value for that key.
     * @throws ClassCastException If the value is not a {@link Integer}.
     */
    public Integer getInteger(String key, Integer dflt) {
        Integer val = (Integer)get(key);

        return val != null || containsKey(key) ? val : dflt;
    }

    /**
     * Get the long value with the specified key.
     *
     * @param key Key.
     * @param dflt Default value, if map does not contain key.
     * @return Value or {@code dflt} if no value for that key.
     * @throws ClassCastException If the value is not a {@link Long}.
     */
    public Long getLong(String key, Long dflt) {
        Long val = (Long)get(key);

        return val != null || containsKey(key) ? val : dflt;
    }

    /**
     * Get the UUID value with the specified key.
     *
     * @param key Key.
     * @return Value or {@code null} if no value for that key.
     * @throws ClassCastException If the value is not a {@link UUID}.
     */
    public UUID getUuid(String key) {
        Object val = get(key);

        if (val == null)
            return null;

        if (val instanceof UUID)
            return (UUID)val;

        if (val instanceof String)
            return UUID.fromString(val.toString());

        throw new ClassCastException("Failed to get UUID from: " + val);
    }

    /**
     * @return String representation of JSON.
     */
    public String encode() {
        return toJson(this);
    }
}

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

package org.apache.ignite.console.dto;

import java.util.Collection;
import io.vertx.core.buffer.Buffer;

/**
 * Build JSON object.
 */
public class JsonBuilder {
    /** */
    private final Buffer buf;

    /** */
    private boolean hasProps;

    /**
     * Constructor.
     */
    public JsonBuilder() {
        buf = Buffer.buffer();
    }

    /**
     * @return {@code this} for chaining.
     */
    public JsonBuilder startObject() {
        buf.appendString("{");

        return this;
    }

    /**
     * @return {@code this} for chaining.
     */
    public JsonBuilder endObject() {
        buf.appendString("}");

        return this;
    }

    /**
     * @param name Property name.
     * @return {@code this} for chaining.
     */
    private JsonBuilder addName(String name) {
        buf
            .appendString("\"")
            .appendString(name)
            .appendString("\"")
            .appendString(":");

        return this;
    }

    /**
     *
     * @param name Property name.
     * @param val Property value.
     * @return {@code this} for chaining.
     */
    public JsonBuilder addProperty(String name, String val) {
        if (hasProps)
            buf.appendString(",");
        else
            hasProps = true;

        addName(name);

        buf.appendString(val);

        return this;
    }

    /**
     * @param data Collection to add as array.
     * @return {@code this} for chaining.
     */
    public JsonBuilder addArray(Collection<? extends DataObject> data) {
        buf.appendString("[");

        int len = buf.length();

        data.forEach(item -> {
            if (buf.length() > len)
                buf.appendString(",");

            buf.appendString(item.json());
        });

        buf.appendString("]");

        return this;
    }

    /**
     *
     * @param name Property name.
     * @param data Collection to add as array.
     * @return {@code this} for chaining.
     */
    public JsonBuilder addArray(String name, Collection<? extends DataObject> data) {
        return addName(name).addArray(data);
    }

    /**
     * @return Buffer with JSON content.
     */
    public Buffer buffer() {
        return buf;
    }
}

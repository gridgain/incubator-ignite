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

import java.util.UUID;
import io.vertx.core.json.JsonObject;

/**
 * DTO for cluster model.
 */
public class Model extends DataObject {
    /** */
    private boolean hasIdx;

    /** */
    private String keyType;

    /** */
    private String valType;

    /**
     * @param json JSON data.
     * @return New instance of model DTO.
     */
    public static Model fromJson(JsonObject json) {
        String id = json.getString("_id");

        if (id == null)
            throw new IllegalStateException("Model ID not found");

        return new Model(
            UUID.fromString(id),
            null,
            false, // TODO IGNITE-5617 DETECT INDEXES !!!
            json.getString("keyType"),
            json.getString("valueType"),
            json.encode()
        );
    }

    /**
     * Full constructor.
     *
     * @param id ID.
     * @param space Space ID.
     * @param hasIdx Model has at least one index.
     * @param keyType Key type name.
     * @param valType Value type name.
     * @param json JSON payload.
     */
    protected Model(UUID id, UUID space, boolean hasIdx, String keyType, String valType, String json) {
        super(id, space, json);

        this.hasIdx = hasIdx;
        this.keyType = keyType;
        this.valType = valType;
    }

    /**
     * @return {@code true} if model has at least one index.
     */
    public boolean hasIndex() {
        return hasIdx;
    }

    /**
     * @return Key type name.
     */
    public String keyType() {
        return keyType;
    }

    /**
     * @return Value type name.
     */
    public String valueType() {
        return valType;
    }

    /** {@inheritDoc} */
    @Override public JsonObject shortView() {
        return new JsonObject()
            .put("_id", _id())
            .put("hasIndex", hasIdx)
            .put("keyType", keyType)
            .put("valueType", valType);
    }
}

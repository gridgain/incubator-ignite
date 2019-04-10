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

package org.apache.ignite.console.services;

import java.util.UUID;
import org.apache.ignite.console.util.JsonObject;

/**
 * Base class for routers.
 */
public abstract class AbstractService {
    /**
     *
     * @param params Params in JSON format.
     * @param key Property name.
     * @return JSON for specified property.
     * @throws IllegalStateException If property not found.
     */
    protected JsonObject getProperty(JsonObject params, String key) {
//        JsonObject prop = params.getJsonObject(key);
//
//        if (prop == null)
//            throw new IllegalStateException("Property not found: " + key);
//
//        return prop;
        throw new IllegalStateException("Not implemented yet");
    }

    /**
     * @param params Params in JSON format.
     * @return User ID.
     * @throws IllegalStateException If user ID not found.
     */
    protected UUID getUserId(JsonObject params) {
        JsonObject user = getProperty(params, "user");

        UUID userId = user.getUuid("id");

        if (userId == null)
            throw new IllegalStateException("User ID not found");

        return userId;
    }

    /**
     * @param rows Number of rows.
     * @return JSON with number of affected rows.
     */
    protected JsonObject rowsAffected(int rows) {
        return new JsonObject()
            .add("rowsAffected", rows);
    }
}

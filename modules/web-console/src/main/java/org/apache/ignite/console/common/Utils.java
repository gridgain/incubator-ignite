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
package org.apache.ignite.console.common;

import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Utilities.
 */
public class Utils {
    /** */
    public static JsonObject EMPTY_OBJ = new JsonObject();

    /**
     * @param cause Error.
     * @return Error message or exception class name.
     */
    public static String errorMessage(Throwable cause) {
        String msg = cause.getMessage();

        return F.isEmpty(msg) ? cause.getClass().getName() : msg;
    }

    /**
     * @param a First set.
     * @param b Second set.
     * @return Elements exists in a and not in b.
     */
    public static TreeSet<UUID> diff(TreeSet<UUID> a, TreeSet<UUID> b) {
        return a.stream().filter(item -> !b.contains(item)).collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * @param json JSON object.
     * @param key Key with IDs.
     * @return Set of IDs.
     */
    public static TreeSet<UUID> idsFromJson(JsonObject json, String key) {
        TreeSet<UUID> res = new TreeSet<>();

        JsonArray ids = json.getJsonArray(key);

        if (ids != null) {
            for (int i = 0; i < ids.size(); i++)
                res.add(UUID.fromString(ids.getString(i)));
        }

        return res;
    }

    /**
     * @param json JSON object.
     * @param path Path.
     * @param def the default value to use if the entry is not present.
     * @return the value or {@code def} if no entry present.
     */
    public static boolean getBoolean(JsonObject json, String path, boolean def) {
        String[] keys = path.split("\\.");

        for (int i = 0; i < keys.length - 1; i++)
            json = json.getJsonObject(keys[i], EMPTY_OBJ);

        return json.getBoolean(keys[keys.length - 1], def);
    }

    /**
     * @param data Collection of DTO objects.
     * @return JSON array.
     */
    public static JsonArray toJsonArray(Collection<? extends DataObject> data) {
        return data.stream().reduce(new JsonArray(), (a, b) -> a.add(new JsonObject(b.json())), JsonArray::addAll);
    }
}

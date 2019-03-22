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
import org.apache.ignite.internal.util.typedef.F;

/**
 * DTO for queries notebook.
 */
public class Notebook extends DataObject {
    /** */
    private String name;

//    /**
//     * @param json JSON data.
//     * @return New instance of model DTO.
//     */
//    public static Notebook fromJson(JsonObject json) {
//        String id = json.getString("_id");
//
//        if (id == null)
//            throw new IllegalStateException("Notebook ID not found");
//
//        String name = json.getString("name");
//
//        if (F.isEmpty(name))
//            throw new IllegalStateException("Notebook name is empty");
//
//        return new Notebook(
//            UUID.fromString(id),
//            json.getString("name"),
//            json.encode()
//        );
//    }

    /**
     * Full constructor.
     *
     * @param id ID.
     * @param name Notebook name.
     * @param json JSON payload.
     */
    public Notebook(UUID id, String name, String json) {
        super(id, json);

        this.name = name;
    }

    /**
     * @return name Notebook name.
     */
    public String name() {
        return name;
    }

//    /** {@inheritDoc} */
//    @Override public JsonObject shortView() {
//        return new JsonObject()
//            .put("_id", _id())
//            .put("name", name);
//    }
}

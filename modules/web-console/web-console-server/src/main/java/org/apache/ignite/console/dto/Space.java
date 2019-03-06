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
 * DTO for User space.
 */
public class Space extends AbstractDto {
    /** */
    private String name;

    /** */
    private UUID owner;

    /** */
    private boolean demo;

    /**
     * Default constructor.
     */
    public Space() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param id Space ID.
     * @param name Space name.
     * @param owner Reference to owner account.
     * @param demo Flag of demo space.
     */
    public Space(
        UUID id,
        String name,
        UUID owner,
        boolean demo
    ) {
        this.id = id;
        this.name = name;
        this.owner = owner;
        this.demo = demo;
    }

    /** {@inheritDoc} */
    public JsonObject toJson() {
        return new JsonObject()
            .put("_id", id.toString())
            .put("name", name)
            .put("owner", owner.toString())
            .put("demo", demo);
    }
}

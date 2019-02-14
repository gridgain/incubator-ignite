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
 * DTO for cluster configuration.
 */
public class Cluster extends DataObject {
    /** */
    private String name;

    /** */
    private String discovery;

    /**
     * Full constructor.
     *
     * @param id ID.
     * @param space Space ID.
     * @param name Cluster name.
     * @param discovery Cluster discovery.
     * @param json JSON payload.
     */
    public Cluster(UUID id, UUID space, String name, String discovery, String json) {
        super(id, space, json);

        this.name = name;
        this.discovery = discovery;
    }

    /**
     * @return name Cluster name.
     */
    public String name() {
        return name;
    }

    /**
     * @return name Cluster discovery.
     */
    public String discovery() {
        return discovery;
    }

    /** {@inheritDoc} */
    @Override public JsonObject shortView() {
        return new JsonObject()
            .put("_id", _id())
            .put("name", name)
            .put("discovery", discovery);
    }
}

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
import org.apache.ignite.console.util.JsonObject;
import org.apache.ignite.igfs.IgfsMode;

import static org.apache.ignite.igfs.IgfsMode.PRIMARY;

/**
 * DTO for cluster IGFS.
 */
public class Igfs extends DataObject {
    /** */
    private String name;

    /** */
    private IgfsMode dfltMode;

    /** */
    private int affGrpSz;

    /**
     * @param json JSON data.
     * @return New instance of IGFS DTO.
     */
    public static Igfs fromJson(JsonObject json) {
        UUID id = json.getUuid("id");

        if (id == null)
            throw new IllegalStateException("IGFS ID not found");

        return new Igfs(
            id,
            json.getString("name"),
            IgfsMode.valueOf(json.getString("defaultMode", PRIMARY.name())),
            json.getInteger("affinityGroupSize", 512),
            json.encode()
        );
    }

    /**
     * Full constructor.
     *
     * @param id ID.
     * @param name IGFS name.
     * @param dfltMode IGFS default mode.
     * @param affGrpSz IGFS size of the group in blocks.
     * @param json JSON payload.
     */
    public Igfs(UUID id, String name, IgfsMode dfltMode, int affGrpSz, String json) {
        super(id, json);

        this.name = name;
        this.dfltMode = dfltMode;
        this.affGrpSz = affGrpSz;
    }

    /**
     * @return IGFS name.
     */
    public String name() {
        return name;
    }

    /**
     * @return IGFS default mode.
     */
    public IgfsMode defaultMode() {
        return dfltMode;
    }

    /**
     * @return IGFS size of the group in blocks.
     */
    public int affinityGroupSize() {
        return affGrpSz;
    }

    /** {@inheritDoc} */
    @Override public JsonObject shortView() {
        return new JsonObject()
            .add("id", getId())
            .add("name", name)
            .add("defaultMode", dfltMode)
            .add("affinityGroupSize", affGrpSz);
    }
}

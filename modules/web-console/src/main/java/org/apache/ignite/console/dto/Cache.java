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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;

/**
 * DTO for cluster cache.
 */
public class Cache extends DataObject {
    /** */
    private String name;

    /** */
    private CacheMode cacheMode;

    /** */
    private CacheAtomicityMode atomicityMode;

    /** */
    private int backups;

    /**
     * Full constructor.
     *
     * @param id ID.
     * @param space Space ID.
     * @param name Cache name.
     * @param json JSON payload.
     */
    public Cache(
        UUID id,
        UUID space,
        String name,
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        int backups,
        String json
    ) {
        super(id, space, json);

        this.name = name;
        this.cacheMode = cacheMode;
        this.atomicityMode = atomicityMode;
        this.backups = backups;
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode cacheMode() {
        return cacheMode;
    }

    /**
     * @return Cache atomicy mode.
     */
    public CacheAtomicityMode atomicityMode() {
        return atomicityMode;
    }

    /**
     * @return Cache backups.
     */
    public int backups() {
        return backups;
    }
}

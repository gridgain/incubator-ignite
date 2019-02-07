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

package org.apache.ignite.console.db;

import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Helper class to support one to many relation.
 */
public class LookupIndex {
    /** */
    private final IgniteCache<UUID, TreeSet<UUID>> cache;

    /**
     * Constructor.
     *
     * @param ignite Ignite.
     * @param parent Parent entity name.
     * @param child Child entity name.
     */
    public LookupIndex(Ignite ignite, String parent, String child) {
        CacheConfiguration<UUID, TreeSet<UUID>> ccfg = new CacheConfiguration<>(parent + "_" + child + "_lookup");
        ccfg.setCacheMode(REPLICATED);
        ccfg.setAtomicityMode(TRANSACTIONAL);

        cache = ignite.getOrCreateCache(ccfg);
    }

    /**
     * @param id
     * @return
     */
    public TreeSet<UUID> getIds(UUID id) {
        TreeSet<UUID> ids = cache.get(id);

        if (ids == null)
            ids = new TreeSet<>();

        return ids;
    }

    /**
     *
     * @param id
     * @param ids
     */
    public void setIds(UUID id, TreeSet<UUID> ids) {
        cache.put(id, ids);
    }
}

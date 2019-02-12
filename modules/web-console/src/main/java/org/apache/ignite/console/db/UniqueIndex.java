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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.CacheHolder;
import org.apache.ignite.console.dto.DataObject;

/**
 * Index for unique constraint.
 */
public class UniqueIndex extends CacheHolder<String, UUID> {
    /** */
    private final String errTemplate;

    /**
     * Constructor.
     *
     * @param ignite Ignite.
     * @param idxName Index name.
     * @param errTemplate Error template.
     */
    public UniqueIndex(Ignite ignite, String idxName, String errTemplate) {
        super(ignite, idxName);

        this.errTemplate = errTemplate;
    }

    /**
     * @param key Key.
     * @param payload Unique payload.
     * @return String to put into cache.
     */
    private String makeKey(UUID key, Object payload) {
        return key.toString() + payload;
    }

    /**
     * Remove value from index.
     *
     * @param key Key.
     * @param payload Unique payload.
     */
    public void removeUniqueKey(UUID key, Object payload) {
        cache.remove(makeKey(key, payload));
    }

    /**
     * Check that value is unique.
     *
     * @param key Key.
     * @param payload Unique payload.
     * @param owner Owner object.
     * @param errArg Argument for substitution to error template.
     */
    public void checkUnique(UUID key, Object payload, DataObject owner, String errArg) {
        UUID prevId = cache.getAndPutIfAbsent(makeKey(key, payload), owner.id());

        if (prevId != null && !owner.id().equals(prevId))
            throw new IllegalStateException(String.format(errTemplate, errArg));
    }
}

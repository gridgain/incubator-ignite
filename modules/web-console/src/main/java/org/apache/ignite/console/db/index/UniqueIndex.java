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

package org.apache.ignite.console.db.index;

import java.util.UUID;
import org.apache.ignite.Ignite;

/**
 * Index for unique constraint.
 */
public class UniqueIndex extends AbstractIndex<String, Boolean> {
    /**
     * Constructor.
     *
     * @param ignite Ignite.
     * @param idxName Index name.
     */
    public UniqueIndex(Ignite ignite, String idxName) {
        super(ignite, idxName);
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
    public void remove(UUID key, Object payload) {
        cache().remove(makeKey(key, payload));
    }

    /**
     * Put key in index if it is not already there.
     *
     * @param key Key.
     * @param payload Unique payload.
     * @return {@code true} if value was added to index.
     */
    public boolean putIfAbsent(UUID key, Object payload) {
        return cache().putIfAbsent(makeKey(key, payload), Boolean.TRUE);
    }
}

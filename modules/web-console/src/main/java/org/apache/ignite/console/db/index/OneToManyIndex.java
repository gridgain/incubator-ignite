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

import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.core.CacheHolder;

/**
 * Index for one to many relation.
 */
public class OneToManyIndex extends CacheHolder<UUID, TreeSet<UUID>> {
    /**
     * Constructor.
     *
     * @param ignite Ignite.
     * @param parent Parent entity name.
     * @param child Child entity name.
     */
    public OneToManyIndex(Ignite ignite, String parent, String child) {
        super(ignite, "wc_" + parent + "_to_" + child + "_idx");
    }

    /**
     * @param parentId Parent ID.
     * @return Set of children IDs.
     */
    public TreeSet<UUID> getIds(UUID parentId) {
        TreeSet<UUID> childrenIds = cache().get(parentId);

        if (childrenIds == null)
            childrenIds = new TreeSet<>();

        return childrenIds;
    }

    /**
     * Put child ID to index.
     *
     * @param parentId Parent ID.
     * @param childId Child ID.
     */
    public void put(UUID parentId, UUID childId) {
        TreeSet<UUID> childrenIds = getIds(parentId);

        childrenIds.add(childId);

        cache().put(parentId, childrenIds);
    }

    /**
     * Remove child ID from index.
     *
     * @param parentId Parent ID.
     * @param childId Child ID.
     */
    public void remove(UUID parentId, UUID childId) {
        TreeSet<UUID> childrenIds = getIds(parentId);

        childrenIds.remove(childId);

        cache().put(parentId, childrenIds);
    }
}

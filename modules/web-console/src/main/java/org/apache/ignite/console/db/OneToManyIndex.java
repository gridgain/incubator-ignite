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

import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;

/**
 * Index for one to many relation.
 */
public class OneToManyIndex extends CacheHolder<UUID, TreeSet<UUID>> {
    /**
     * Constructor.
     *
     * @param ignite Ignite.
     * @param idxName Index name.
     */
    public OneToManyIndex(Ignite ignite, String idxName) {
        super(ignite, idxName);
    }

    /**
     * @param set Set to check.
     * @return Specified set if it is not {@code null} or new empty {@link TreeSet}.
     */
    private TreeSet<UUID> ensure(TreeSet<UUID> set) {
        return (set == null) ? new TreeSet<>() : set;
    }

    /**
     * @param parentId Parent ID.
     * @return Set of children IDs.
     */
    public TreeSet<UUID> load(UUID parentId) {
        return ensure(cache.get(parentId));
    }

    /**
     * Put child ID to index.
     *
     * @param parentId Parent ID.
     * @param child Child ID to add.
     */
    public void add(UUID parentId, UUID child) {
        TreeSet<UUID> childrenIds = load(parentId);

        childrenIds.add(child);

        cache.put(parentId, childrenIds);
    }

    /**
     * Put children IDs to index.
     *
     * @param parent Parent ID.
     * @param childrenToAdd Children IDs to add.
     */
    public void addAll(UUID parent, Set<UUID> childrenToAdd) {
        TreeSet<UUID> children = load(parent);

        children.addAll(childrenToAdd);

        cache.put(parent, children);
    }

    /**
     * Remove child ID from index.
     *
     * @param parent Parent ID.
     * @param child Child ID to remove.
     */
    public void remove(UUID parent, UUID child) {
        TreeSet<UUID> children = load(parent);

        children.remove(child);

        cache.put(parent, children);
    }

    /**
     * Remove children IDs from index.
     *
     * @param parent Parent ID.
     * @param childrenToRmv Children IDs to remove.
     */
    public void removeAll(UUID parent, TreeSet<UUID> childrenToRmv) {
        TreeSet<UUID> children = load(parent);

        children.removeAll(childrenToRmv);

        cache.put(parent, children);
    }

    /**
     * Delete entry from index.
     *
     * @param parent Parent ID to delete.
     * @return Children IDs associated with parent ID.
     */
    public TreeSet<UUID> delete(UUID parent) {
        return ensure(cache.getAndRemove(parent));
    }
}

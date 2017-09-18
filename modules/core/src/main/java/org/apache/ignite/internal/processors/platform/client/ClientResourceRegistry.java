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

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.GridConcurrentHashSet;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-connection resource registry.
 */
public class ClientResourceRegistry {
    /** Handles. */
    private final Map<Long, Object> res = new ConcurrentHashMap<>();

    /** Cursors. */
    private final Collection<Object> cursors = new GridConcurrentHashSet<>();

    /** ID generator. */
    private final AtomicLong idGen = new AtomicLong();

    /** Max cursors. */
    private final int maxCursors;

    /**
     * Ctor.
     *
     * @param maxCursors Maximum cursors.
     */
    ClientResourceRegistry(int maxCursors) {
        this.maxCursors = maxCursors;
    }

    /**
     * Allocates server handle for an object.
     *
     * @param obj Object.
     * @return Handle.
     */
    public long put(Object obj, boolean isCursor) {
        if (isCursor) {
            if (cursors.size() >= maxCursors) {
                throw new IgniteException("Too many open cursors. Close active cursors or increase " +
                        "ClientConnectorConfiguration.maxOpenCursorsPerConnection.");
            }

            cursors.add(obj);
        }

        long id = idGen.incrementAndGet();

        res.put(id, obj);

        return id;
    }

    /**
     * Gets the object by handle.
     *
     * @param hnd Handle.
     * @param <T> Object type.
     * @return Object.
     */
    @SuppressWarnings("unchecked")
    public <T> T get(long hnd) {
        Object obj = res.get(hnd);

        if (obj == null)
            throw new IgniteException("Failed to find resource with id: " + hnd);

        return (T) obj;
    }

    /**
     * Releases the handle.
     *
     * @param hnd Handle.
     */
    public void release(long hnd) {
        Object obj = res.remove(hnd);

        if (obj == null)
            throw new IgniteException("Failed to find resource with id: " + hnd);

        cursors.remove(obj);

        closeIfNeeded(obj);
    }

    /**
     * Cleans all handles and closes all ClientCloseableResources.
     */
    public void clean() {
        for (Map.Entry e : res.entrySet())
            closeIfNeeded(e.getValue());
    }

    /**
     * Close resource if needed.
     *
     * @param res Resource.
     */
    private static void closeIfNeeded(Object res) {
        if (res instanceof ClientCloseableResource)
            ((ClientCloseableResource)res).close();
    }
}

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

package org.apache.ignite.internal.processors.query.h2.affinity;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Partition resolver.
 */
public class PartitionTableDescriptor {
    /** Cache name. */
    private final String cacheName;

    /** Table name. */
    private final String tblName;

    /**
     * Constructor.
     *
     * @param cacheName Cache name.
     * @param tblName Table name.
     */
    public PartitionTableDescriptor(String cacheName, String tblName) {
        this.cacheName = cacheName;
        this.tblName = tblName;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * cacheName.hashCode() + tblName.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == this)
            return true;

        if (o.getClass() != getClass())
            return false;

        PartitionTableDescriptor other = (PartitionTableDescriptor)o;

        return F.eq(cacheName, other.cacheName) && F.eq(tblName, other.tblName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionTableDescriptor.class, this);
    }
}

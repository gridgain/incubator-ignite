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

package org.apache.ignite.client;

import java.util.*;

/**
 * Configuration defining various aspects of cache keys without explicit usage of annotations on user classes.
 */
public final class CacheKeyConfiguration {
    /** Type name. */
    private String typeName;

    /** Affinity key field name. */
    private String affKeyFieldName;

    /**
     * Creates cache key configuration with given type name and affinity field name.
     *
     * @param typeName Type name.
     * @param affKeyFieldName Affinity field name.
     */
    public CacheKeyConfiguration(String typeName, String affKeyFieldName) {
        this.typeName = typeName;
        this.affKeyFieldName = affKeyFieldName;
    }

    /**
     * @return Type name for which affinity field name is being defined.
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * @param typeName Type name for which affinity field name is being defined.
     */
    public CacheKeyConfiguration setTypeName(String typeName) {
        this.typeName = typeName;

        return this;
    }

    /**
     * @return Affinity key field name.
     */
    public String getAffinityKeyFieldName() {
        return affKeyFieldName;
    }

    /**
     * @param affKeyFieldName Affinity key field name.
     */
    public CacheKeyConfiguration setAffinityKeyFieldName(String affKeyFieldName) {
        this.affKeyFieldName = affKeyFieldName;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof CacheKeyConfiguration))
            return false;

        CacheKeyConfiguration other = (CacheKeyConfiguration)obj;

        return Objects.equals(typeName, other.typeName) && Objects.equals(affKeyFieldName, other.affKeyFieldName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 11;

        if (typeName != null)
            res = 31 * res + typeName.hashCode();

        if (affKeyFieldName != null)
            res = 31 * res + affKeyFieldName.hashCode();

        return res;
    }
}

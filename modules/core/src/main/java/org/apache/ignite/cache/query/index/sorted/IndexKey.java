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

package org.apache.ignite.cache.query.index.sorted;

import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a complex index key.
 */
public interface IndexKey {
    /**
     * @return underlying keys.
     */
    public Object[] keys();

    /**
     * @return Cache row if index key represents an existing row or {@code null} if it is only a user query.
     */
    public @Nullable CacheDataRow cacheRow();
}

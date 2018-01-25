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

package org.apache.ignite.internal.processors.bulkload;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.List;

/**
 * Converter, which transforms the list of strings parsed from the input stream to the key+value entry to add to
 * the cache.
 */
public interface BulkLoadEntryConverter {

    /**
     * Transforms the list of strings parsed from the input stream to the key+value entry to add to
     * the cache.
     *
     * @param record The input record as a list of field values.
     * @return key+value pair to add to the cache.
     */
    IgniteBiTuple<?, ?> convertRecord(List<?> record) throws IgniteCheckedException;
}

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

package org.apache.ignite.binary;

import org.apache.ignite.IgniteCheckedException;

/** Enum metadata */
public interface EnumMetadata {
    /**
     * Gets enum constant name given its ordinal value.
     *
     * @param ord Enum constant ordinal value.
     * @return Enum constant name.
     * @throws IgniteCheckedException
     */
    String getNameByOrdinal(int ord) throws IgniteCheckedException;

    /**
     * Gets enum constant ordinal value given its name.
     *
     * @param name Enum constant name.
     * @return Enum constant ordinal value.
     * @throws IgniteCheckedException
     */
    int getOrdinalByName(String name) throws IgniteCheckedException;

    /**
     * Performs merge of {@code this} enum metadata with other instance.
     *
     * @param other instance of enum metadata to merge with {@code this}.
     * @return Combined metadata instance.
     */
    EnumMetadata merge(EnumMetadata other);
}

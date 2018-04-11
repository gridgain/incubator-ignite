/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.internal.commandline.tx;

import java.io.Serializable;
import java.util.Collection;
import java.util.regex.Pattern;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CollectTxInfoArguments implements Serializable {

    private final Collection<String> consistentIds;
    /** transaction size */
    private final long size;
    /** Duration to retrieve transaction information. */
    private final long duration;
    /** transaction label regex pattern */
    @Nullable private final Pattern lbPat;
    private boolean clients;

    /**
     * @param consistentIds
     * @param duration Duration.
     * @param size Size.
     * @param lbPat Label regex pattern.
     */
    public CollectTxInfoArguments(boolean clients, Collection<String> consistentIds, long duration,
        long size, @Nullable Pattern lbPat) {
        this.clients = clients;

        this.consistentIds = consistentIds;

        this.duration = duration;

        this.size = size;

        this.lbPat = lbPat;
    }

    public boolean clients() {
        return clients;
    }

    public long duration() {
        return duration;
    }

    public long size() {
        return size;
    }

    @Nullable public Pattern labelPattern() {
        return lbPat;
    }

    public Collection<String> consistentIds() {
        return consistentIds;
    }
}

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;

public class NodeDTO implements Serializable {

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final UUID uuid;

    /** */
    private final Object consistentId;

    /** */
    private final Collection<String> addrs;

    /** */
    private final Collection<String> hostNames;

    /** */
    private final boolean client;

    public NodeDTO(UUID uuid, Object consistentId, Collection<String> addrs, Collection<String> hostNames,
        boolean client) {
        this.uuid = uuid;

        this.consistentId = consistentId;

        this.addrs = new ArrayList<>(addrs);

        this.hostNames = new ArrayList<>(hostNames);
        this.client = client;
    }

    public static NodeDTO of(ClusterNode node) {
        return new NodeDTO(node.id(), node.consistentId(), node.addresses(), node.hostNames(), node.isClient());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "{" + "uuid=" + uuid +
            ", consistentId=" + consistentId +
            ", client=" + client +
            ", addrs=" + addrs +
            ", hostNames=" + hostNames +
            '}';
    }
}

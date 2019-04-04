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

package org.apache.ignite.console.websocket;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Agent descriptor POJO.
 */
public class AgentInfo {
    /** */
    private String agentId;

    /** */
    private Set<String> toks;

    /**
     * Default constructor for serialization.
     */
    public AgentInfo() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param agentId Agent ID.
     * @param toks Tokens.
     */
    public AgentInfo(String agentId, List<String> toks) {
        this.agentId = agentId;

        this.toks = new HashSet<>();
        this.toks.addAll(toks);
    }

    /**
     * @return Agent ID.
     */
    public String getAgentId() {
        return agentId;
    }

    /**
     * @param agentId Agent ID.
     */
    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    /**
     * @return Tokens.
     */
    public Set<String> getTokens() {
        return toks;
    }

    /**
     * @param toks Tokens.
     */
    public void setTokens(Set<String> toks) {
        this.toks = toks;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AgentInfo.class, this);
    }
}

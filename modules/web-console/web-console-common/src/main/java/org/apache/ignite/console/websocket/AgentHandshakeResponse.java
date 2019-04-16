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

import java.util.Set;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Handshake request for Web Console Agent.
 */
public class AgentHandshakeResponse {
    /** */
    private String err;

    /** */
    @GridToStringInclude
    private Set<String> toks;

    /**
     * @param err Error message.
     */
    public static AgentHandshakeResponse error(String err) {
        AgentHandshakeResponse res = new AgentHandshakeResponse();

        res.setError(err);

        return res;
    }

    /**
     * @param toks Tokens.
     */
    public static AgentHandshakeResponse tokens(Set<String> toks) {
        AgentHandshakeResponse res = new AgentHandshakeResponse();

        res.setTokens(toks);

        return res;
    }

    /**
     * @return Error message.
     */
    public String getError() {
        return err;
    }

    /**
     * @param err Error message.
     */
    public void setError(String err) {
        this.err = err;
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
        return S.toString(AgentHandshakeResponse.class, this);
    }
}

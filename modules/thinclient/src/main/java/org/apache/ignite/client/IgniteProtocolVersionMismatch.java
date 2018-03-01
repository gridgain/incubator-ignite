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

/**
 * Indicates protocol version mismatch between the client and server.
 */
public class IgniteProtocolVersionMismatch extends IgniteClientError {
    /** Serial version uid. */
    private static final long serialVersionUID = 7338239988823521260L;

    /** Client version. */
    private final ProtocolVersion clientVer;

    /** Server version. */
    private final ProtocolVersion srvVer;

    /**
     * Constructor.
     */
    IgniteProtocolVersionMismatch(ProtocolVersion clientVer, ProtocolVersion srvVer, String srvDetails) {
        super(
            SystemEvent.PROTOCOL_VERSION_MISMATCH,
            String.format(
                "Protocol version mismatch: client %s / server %s. Server details: %s",
                clientVer,
                srvVer,
                srvDetails
            )
        );

        this.clientVer = clientVer;
        this.srvVer = srvVer;
    }

    /**
     * @return Client protocol version.
     */
    public ProtocolVersion getClientVersion() {
        return clientVer;
    }

    /**
     * @return Server protocol version.
     */
    public ProtocolVersion getServerVersion() {
        return srvVer;
    }
}

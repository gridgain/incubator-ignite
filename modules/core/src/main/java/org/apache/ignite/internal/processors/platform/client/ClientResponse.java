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

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;

import static org.apache.ignite.internal.processors.platform.client.ClientConnectionContext.VER_1_3_0;

/**
 * Thin client response.
 */
public class ClientResponse extends ClientListenerResponse {
    /** Request id. */
    private final long reqId;

    /**
     * Constructor.
     *
     * @param reqId Request id.
     */
    public ClientResponse(long reqId) {
        super(ClientStatus.SUCCESS, null);

        this.reqId = reqId;
    }

    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param err Error message.
     */
    public ClientResponse(long reqId, String err) {
        super(ClientStatus.FAILED, err);

        this.reqId = reqId;
    }

    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param status Status code.
     * @param err Error message.
     */
    public ClientResponse(long reqId, int status, String err) {
        super(status, err);

        this.reqId = reqId;
    }

    /**
     * Encodes the response data. Used when response result depends on the specific affinity version.
     * @param ctx Connection context.
     * @param writer Writer.
     * @param affinityVer Affinity version.
     */
    public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer,
        ClientAffinityTopologyVersion affinityVer) {
        writer.writeLong(reqId);

        ClientListenerProtocolVersion ver = ctx.currentVersion();

        assert ver != null;

        if (ver.compareTo(VER_1_3_0) >= 0) {
            boolean error = status() != ClientStatus.SUCCESS;

            short flags = makeFlags(error, affinityVer.isChanged());

            writer.writeShort(flags);

            if (affinityVer.isChanged())
                affinityVer.write(writer);

            // If no return flag is set, no additional data is written to a payload.
            if (!error)
                return;
        }

        writer.writeInt(status());

        if (status() != ClientStatus.SUCCESS) {
            writer.writeString(error());
        }
    }

    /**
     * Encodes the response data.
     * @param ctx Connection context.
     * @param writer Writer.
     */
    public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        encode(ctx, writer, ctx.checkAffinityTopologyVersion());
    }

    /**
     * Gets the request id.
     *
     * @return Request id.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @return Flags for response message.
     * @param error Error flag.
     * @param topologyChanged Affinity topology changed flag.
     */
    private static short makeFlags(boolean error, boolean topologyChanged) {
        short flags = 0;

        if (error)
            flags |= ClientFlag.ERROR;

        if (topologyChanged)
            flags |= ClientFlag.AFFINITY_TOPOLOGY_CHANGED;

        return flags;
    }
}

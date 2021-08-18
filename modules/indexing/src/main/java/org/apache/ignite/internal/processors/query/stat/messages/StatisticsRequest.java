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

package org.apache.ignite.internal.processors.query.stat.messages;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.stat.StatisticsType;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Request for statistics.
 */
public class StatisticsRequest implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 186;

    /** Gathering id. */
    private UUID reqId;

    /** Key to supply statistics by. */
    private StatisticsKeyMessage key;

    /** Type of required statistcs. */
    private StatisticsType type;

    /** For local statistics request - column config versions map: name to version. */
    private Map<String, Long> versions;

    /** For local statistics request - version to gather statistics by. */
    private AffinityTopologyVersion topVer;

    /**
     * Constructor.
     */
    public StatisticsRequest() {
    }

    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param key Statistics key to get statistics by.
     * @param type Required statistics type.
     * @param topVer Topology version to get statistics by.
     * @param versions Map of statistics version to column name to ensure actual statistics aquired.
     */
    public StatisticsRequest(
        UUID reqId,
        StatisticsKeyMessage key,
        StatisticsType type,
        AffinityTopologyVersion topVer,
        Map<String, Long> versions
    ) {
        this.reqId = reqId;
        this.key = key;
        this.type = type;
        this.topVer = topVer;
        this.versions = versions;
    }

    /** Request id. */
    public UUID reqId() {
        return reqId;
    }

    /**
     * @return Key for required statitics.
     */
    public StatisticsKeyMessage key() {
        return key;
    }

    /**
     * @return Required statistics type.
     */
    public StatisticsType type() {
        return type;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topVer() {
        return topVer;
    }

    /**
     * @return Column name to version map.
     */
    public Map<String, Long> versions() {
        return versions;
    }

    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeUuid("reqId", reqId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeByte("type", type != null ? (byte)type.ordinal() : -1))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                reqId = reader.readUuid("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                byte typeOrd;

                typeOrd = reader.readByte("type");

                if (!reader.isLastRead())
                    return false;

                type = StatisticsType.fromOrdinal(typeOrd);

                reader.incrementState();

        }

        return reader.afterMessageRead(StatisticsRequest.class);
    }

    @Override public short directType() {
        return 0;
    }

    @Override public byte fieldsCount() {
        return 3;
    }

    @Override public void onAckReceived() {

    }
}

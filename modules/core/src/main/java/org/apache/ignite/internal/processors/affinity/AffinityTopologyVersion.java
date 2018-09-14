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

package org.apache.ignite.internal.processors.affinity;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class AffinityTopologyVersion implements Comparable<AffinityTopologyVersion>, Externalizable, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final AffinityTopologyVersion NONE = new AffinityTopologyVersion(-1, 0, 0);

    /** */
    public static final AffinityTopologyVersion ZERO = new AffinityTopologyVersion(0, 0, 0);

    /** */
    private long topVer;

    /** */
    private long majorAffVer;

    /** */
    private int minorAffVer;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public AffinityTopologyVersion() {
        // No-op.
    }
//
//    /**
//     * @param topVer Topology version.
//     */
//    public AffinityTopologyVersion(long topVer) {
//        this.topVer = topVer;
//    }
//
//    /**
//     * @param topVer Topology version.
//     * @param minorAffVer Minor topology version.
//     */
//    public AffinityTopologyVersion(
//        long topVer,
//        int minorAffVer
//    ) {
//        this.topVer = topVer;
//        this.minorAffVer = minorAffVer;
//    }

    /**
     * @param topVer Topology version.
     * @param minorAffVer Minor topology version.
     */
    public AffinityTopologyVersion(
        long topVer,
        long majorAffVer,
        int minorAffVer
    ) {
        this.topVer = topVer;
        this.majorAffVer = majorAffVer;
        this.minorAffVer = minorAffVer;
    }

    /**
     * @return {@code True} if this is real topology version (neither {@link #NONE} nor {@link #ZERO}.
     */
    public boolean initialized() {
        return topVer > 0;
    }

    public AffinityTopologyVersion nextTopologyAffinityVersion() {
        assert topVer > 0;

        return majorAffVer > 0 ?
            new AffinityTopologyVersion(topVer + 1, majorAffVer + 1, 0) :
            new AffinityTopologyVersion(topVer + 1, 0, 0);
    }

    public AffinityTopologyVersion nextTopologyVersion() {
        assert topVer > 0;

        return new AffinityTopologyVersion(topVer + 1, majorAffVer, minorAffVer);
    }

    public AffinityTopologyVersion nextMajorAffinityVersion() {
        assert topVer > 0;
        assert majorAffVer > 0;

        return new AffinityTopologyVersion(topVer, majorAffVer + 1, 0);
    }

    /**
     * @return Topology version with incremented minor version.
     */
    public AffinityTopologyVersion nextMinorAffinityVersion() {
        assert topVer > 0;

        return new AffinityTopologyVersion(topVer, majorAffVer, minorAffVer + 1);
    }

    /**
     * @return Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    public long affinityVersion() {
        return majorAffVer;
    }

    /**
     * @return Minor topology version.
     */
    public int minorAffinityVersion() {
        return minorAffVer;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(AffinityTopologyVersion o) {
        int cmp = Long.compare(topVer, o.topVer);

        if (cmp == 0)
            return Integer.compare(minorAffVer, o.minorAffVer);

        return cmp;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof AffinityTopologyVersion))
            return false;

        AffinityTopologyVersion that = (AffinityTopologyVersion)o;

        return minorAffVer == that.minorAffVer && topVer == that.topVer && majorAffVer == that.majorAffVer;
    }

    @Override public int hashCode() {
        return Objects.hash(topVer, majorAffVer, minorAffVer);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        assert false : "???";

        out.writeLong(topVer);
        out.writeInt(minorAffVer);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        assert false : "???";

        topVer = in.readLong();
        minorAffVer = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeLong("majorAffVer", majorAffVer))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeInt("minorAffVer", minorAffVer))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong("topVer", topVer))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                majorAffVer = reader.readLong("majorAffVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                minorAffVer = reader.readInt("minorAffVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(AffinityTopologyVersion.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 111;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AffinityTopologyVersion.class, this);
    }
}

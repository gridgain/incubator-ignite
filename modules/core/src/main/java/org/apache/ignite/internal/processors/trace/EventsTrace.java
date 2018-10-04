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

package org.apache.ignite.internal.processors.trace;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class EventsTrace implements Message, IgniteTraceAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private byte[] traceIds;

    /** */
    private long[] traceStamps;

    /** */
    private String[] traceThreads;

    /** */
    private int numTracePoints;

    /** */
    @GridDirectCollection(UUID.class)
    private List<UUID> rmtNodes;

    /** */
    @GridDirectCollection(EventsTrace.class)
    private List<EventsTrace> rmtTraces;

    /**
     * @param point Trace point.
     */
    @Override public synchronized void recordTracePoint(TracePoint point) {
        if (traceIds == null || numTracePoints == traceIds.length)
            expandTracePoints();

        traceIds[numTracePoints] = (byte)point.ordinal();
        traceStamps[numTracePoints] = System.nanoTime() / 1000;

        String curThread = Thread.currentThread().getName();

        if (numTracePoints == 0 || !F.eq(traceThreads[numTracePoints - 1], curThread))
            traceThreads[numTracePoints] = curThread;

        numTracePoints++;
    }

    /**
     * @param rmtNode Remote node to collect.
     * @param rmtTrace Remote trace to collect.
     */
    public synchronized void addRemoteTrace(UUID rmtNode, EventsTrace rmtTrace) {
        if (rmtNodes == null) {
            assert rmtTraces == null;

            rmtNodes = new ArrayList<>(3);
            rmtTraces = new ArrayList<>(3);
        }

        rmtNodes.add(rmtNode);
        rmtTraces.add(rmtTrace);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -62;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /**
     *
     */
    private void expandTracePoints() {
        if (traceIds == null) {
            assert traceStamps == null;

            traceIds = new byte[4];
            traceStamps = new long[4];
            traceThreads = new String[4];
        }
        else {
            byte[] cp = new byte[traceIds.length * 2];

            System.arraycopy(traceIds, 0, cp, 0, traceIds.length);

            traceIds = cp;

            long[] stampCp = new long[traceStamps.length * 2];

            System.arraycopy(traceStamps, 0, stampCp, 0, traceStamps.length);

            traceStamps = stampCp;

            String[] threadsCp = new String[traceThreads.length * 2];

            System.arraycopy(traceThreads, 0, threadsCp, 0, traceThreads.length);

            traceThreads = threadsCp;
        }
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 6;
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
                if (!writer.writeInt("numTracePoints", numTracePoints))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeCollection("rmtNodes", rmtNodes, MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeCollection("rmtTraces", rmtTraces, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeByteArray("traceIds", traceIds))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeLongArray("traceStamps", traceStamps))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeObjectArray("traceThreads", traceThreads, MessageCollectionItemType.STRING))
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
                numTracePoints = reader.readInt("numTracePoints");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                rmtNodes = reader.readCollection("rmtNodes", MessageCollectionItemType.UUID);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                rmtTraces = reader.readCollection("rmtTraces", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                traceIds = reader.readByteArray("traceIds");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                traceStamps = reader.readLongArray("traceStamps");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                traceThreads = reader.readObjectArray("traceThreads", MessageCollectionItemType.STRING, String.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(EventsTrace.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();

        toString(sb, null, "", 0L);

        return sb.toString();
    }

    /**
     * @param sb String builder to append data to. Used for nested event printout.
     * @param nodeId Node ID if tracing remote node response.
     * @param ident Printout ident.
     * @param base Base event timestamp.
     */
    protected void toString(StringBuilder sb, UUID nodeId, String ident, long base) {
        sb.append(ident).append("Events trace ");

        if (nodeId != null) {
            sb.append("[").append(nodeId).append("][");
        }
        else
            sb.append("[");

        sb.append("\n");

        long firstNioRcv = 0;
        long lastNioSnd = 0;
        long firstLsnrInvoke = 0;

        boolean calcDelta = false;

        long updBase = 0;
        String thread = null;

        for (int i = 0; i < numTracePoints; i++) {
            TracePoint tp = TracePoint.fromOrdinal(traceIds[i]);

            String traceThread = traceThreads[i];

            if (traceThread == null)
                traceThread = thread;
            else
                thread = traceThread;

            sb.append(ident).append("  ").append(tp).append("[").append(traceThread).append("]");

            if (calcDelta)
                sb.append(" (prev +").append(traceStamps[i] - traceStamps[i - 1]).append("us)");
            else if (i == 0 && base != 0)
                sb.append(" (base +").append(traceStamps[0] - base).append("us)");

            sb.append("\n");

            if (tp == TracePoint.MSG_NIO_RECEIVE) {
                if (firstNioRcv == 0)
                    firstNioRcv = traceStamps[i];

                calcDelta = true;
            }
            else if (tp == TracePoint.MSG_NIO_SEND) {
                lastNioSnd = traceStamps[i];

                calcDelta = false;
            }
            else if (tp == TracePoint.MSG_LISTENER_INVOKE) {
                if (firstLsnrInvoke == 0)
                    firstLsnrInvoke = traceStamps[i];
            }
            else
                calcDelta = true;

            if (tp == TracePoint.MSG_LISTENER_INVOKE && updBase == 0)
                updBase = traceStamps[i];
        }

        if (updBase == 0 && numTracePoints > 0)
            updBase = traceStamps[0];

        long latency = traceStamps[traceStamps.length - 1] - traceStamps[0];

        sb.append(ident).append("  Total trace time: ")
            .append(latency).append("us\n");

        if (nodeId != null) {
            sb.append(ident).append("  Listener time: ").append(lastNioSnd - firstLsnrInvoke).append("us\n");
            sb.append(ident).append("  Estimated wire time: ").append(latency - (lastNioSnd - firstNioRcv))
                .append("us\n");
        }

        if (!F.isEmpty(rmtNodes)) {
            for (int i = 0; i < rmtNodes.size(); i++) {
                UUID rmtNodeId = rmtNodes.get(i);

                EventsTrace rmtTrace = rmtTraces.get(i);

                rmtTrace.toString(sb, rmtNodeId, ident + "  ", updBase);
            }
        }

        sb.append(ident).append("]\n");
    }
}

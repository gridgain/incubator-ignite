/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class GridNearTxQueryResultsEnlistRequest extends GridCacheIdMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final byte MERGE = 0;

    /** */
    private static final byte INSERT = 1;

    /** */
    private static final byte UPDATE = 2;

    /** */
    private static final byte DELETE = 3;

    /** */
    private long threadId;

    /** */
    private IgniteUuid futId;

    /** */
    private boolean clientFirst;

    /** */
    private int miniId;

    /** */
    private UUID subjId;

    /** */
    private AffinityTopologyVersion topVer;

    /** */
    private GridCacheVersion lockVer;

    /** */
    private MvccSnapshot mvccSnapshot;

    /** */
    private long timeout;

    /** */
    private int taskNameHash;

    /** Update mode. */
    private byte mode;

    /** */
    @GridDirectTransient
    private Collection<IgniteBiTuple> rows;

    /** */
    @GridToStringExclude
    private KeyCacheObject[] keys;

    /**  */
    @GridToStringExclude
    private CacheObject[] values;

    /** */
    private GridCacheOperation op;

    /**
     * Default constructor.
     */
    public GridNearTxQueryResultsEnlistRequest() {
        // No-op.
    }

    /**
     *
     * @param cacheId
     * @param threadId
     * @param futId
     * @param miniId
     * @param subjId
     * @param topVer
     * @param lockVer
     * @param mvccSnapshot
     * @param clientFirst
     * @param timeout
     */
    public GridNearTxQueryResultsEnlistRequest(int cacheId,
        long threadId,
        IgniteUuid futId,
        int miniId,
        UUID subjId,
        AffinityTopologyVersion topVer,
        GridCacheVersion lockVer,
        MvccSnapshot mvccSnapshot,
        boolean clientFirst,
        long timeout,
        int taskNameHash,
        byte mode,
        Collection<IgniteBiTuple> rows,
        GridCacheOperation op) {
        this.cacheId = cacheId;
        this.threadId = threadId;
        this.futId = futId;
        this.miniId = miniId;
        this.subjId = subjId;
        this.topVer = topVer;
        this.lockVer = lockVer;
        this.mvccSnapshot = mvccSnapshot;
        this.clientFirst = clientFirst;
        this.timeout = timeout;
        this.taskNameHash = taskNameHash;
        this.mode = mode;
        this.rows = rows;
        this.op = op;
    }

    /**
     * @return Thread id.
     */
    public long threadId() {
        return threadId;
    }

    /**
     * @return Future id.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future ID.
     */
    public int miniId() {
        return miniId;
    }

    /**
     * @return Subject id.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Lock version.
     */
    public GridCacheVersion version() {
        return lockVer;
    }

    /**
     * @return MVCC snapshot.
     */
    public MvccSnapshot mvccSnapshot() {
        return mvccSnapshot;
    }

    /**
     * @return Timeout milliseconds.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return {@code True} if this is the first client request.
     */
    public boolean firstClientRequest() {
        return clientFirst;
    }

    /**
     * @return
     */
    public byte mode() {
        return mode;
    }

    /**
     * @return
     */
    public Collection<IgniteBiTuple> rows() {
        return rows;
    }

    /**
     *
     * @return
     */
    public GridCacheOperation operation() {
        return op;
    }


    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);
        CacheObjectContext objCtx = cctx.cacheObjectContext();

        if (rows != null && keys == null) {
            keys = new KeyCacheObject[rows.size()];
            values = new CacheObject[keys.length];

            int i = 0;

            for (IgniteBiTuple row : rows) {
                keys[i] = cctx.toCacheKeyObject(row.getKey());
                keys[i].prepareMarshal(objCtx);

                values[i] = cctx.toCacheObject(row.getValue());

                i++;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (keys != null) {
            rows = new ArrayList<>(keys.length);

            for (int i = 0; i < keys.length; i++) {
                keys[i].finishUnmarshal(ctx.cacheContext(cacheId).cacheObjectContext(), ldr);
                rows.add(new IgniteBiTuple<>(keys[i], values[i]));
            }

            keys = null;
            values = null;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeBoolean("clientFirst", clientFirst))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeObjectArray("keys", keys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMessage("lockVer", lockVer))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeInt("miniId", miniId))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeByte("mode", mode))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMessage("mvccSnapshot", mvccSnapshot))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeByte("op", op != null ? (byte)op.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeLong("threadId", threadId))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeLong("timeout", timeout))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeObjectArray("values", values, MessageCollectionItemType.MSG))
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

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 3:
                clientFirst = reader.readBoolean("clientFirst");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                keys = reader.readObjectArray("keys", MessageCollectionItemType.MSG, KeyCacheObject.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                lockVer = reader.readMessage("lockVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                mode = reader.readByte("mode");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                mvccSnapshot = reader.readMessage("mvccSnapshot");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                byte opOrd;

                opOrd = reader.readByte("op");

                if (!reader.isLastRead())
                    return false;

                op = GridCacheOperation.fromOrdinal(opOrd);

                reader.incrementState();

            case 11:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                threadId = reader.readLong("threadId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                timeout = reader.readLong("timeout");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                values = reader.readObjectArray("values", MessageCollectionItemType.MSG, CacheObject.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearTxQueryResultsEnlistRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 17;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 149;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxQueryResultsEnlistRequest.class, this);
    }
}

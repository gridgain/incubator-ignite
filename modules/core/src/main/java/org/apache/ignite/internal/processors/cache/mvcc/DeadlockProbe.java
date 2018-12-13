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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Probe message travelling between transactions (from waiting to blocking) during deadlock detection.
 * @see DdCollaborator
 */
public class DeadlockProbe implements MvccMessage {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    private GridCacheVersion initiatorVer;
    /** */
    private GridCacheVersion waitingVer;
    /** */
    private GridCacheVersion blockerVer;

    /** */
    public DeadlockProbe() {
    }

    /** */
    public DeadlockProbe(GridCacheVersion initiatorVer, GridCacheVersion waitingVer, GridCacheVersion blockerVer) {
        this.initiatorVer = initiatorVer;
        this.waitingVer = waitingVer;
        this.blockerVer = blockerVer;
    }

    /**
     * @return Identifier of a transaction started a deadlock detection process.
     */
    public GridCacheVersion initiatorVersion() {
        return initiatorVer;
    }

    /**
     * @return Identifier of a transaction identified as waiting during deadlock detection.
     */
    public GridCacheVersion waitingVersion() {
        // t0d0 why do we need that version?
        return waitingVer;
    }

    /**
     * @return Identifier of a transaction identified as blocking another (waiting)
     * transction during deadlock deteciton.
     */
    public GridCacheVersion blockerVersion() {
        return blockerVer;
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
                if (!writer.writeMessage("blockerVer", blockerVer))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("initiatorVer", initiatorVer))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMessage("waitingVer", waitingVer))
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
                blockerVer = reader.readMessage("blockerVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                initiatorVer = reader.readMessage("initiatorVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                waitingVer = reader.readMessage("waitingVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(DeadlockProbe.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 167;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
    }

    /** {@inheritDoc} */
    @Override public boolean waitForCoordinatorInit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean processedFromNioThread() {
        return false;
    }
}

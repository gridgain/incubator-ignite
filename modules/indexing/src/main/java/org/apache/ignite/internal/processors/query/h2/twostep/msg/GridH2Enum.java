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

package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueEnum;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.h2.value.Value;

import java.nio.ByteBuffer;


/**
 * H2 Enum message
 */
public class GridH2Enum  extends GridH2ValueMessage {
    /** */
    private int typeId;

    /** */
    private int ordinal;

    /** */
    public GridH2Enum() {
        //No-op
    }

    /** */
    public GridH2Enum(Value val) {
        assert val instanceof GridH2ValueEnum;
        typeId = val.getType();
        ordinal = val.getInt();
    }

    /** {@inheritDoc} */
    @Override public Value value(GridKernalContext ctx) throws IgniteCheckedException {
        return new GridH2ValueEnum(ctx, typeId, ordinal, null, null);
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
            case 0:
                if (!writer.writeInt("ordinal", ordinal))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeInt("typeId", typeId))
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
            case 0:
                ordinal = reader.readInt("ordinal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                typeId = reader.readInt("typeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridH2Enum.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -60;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "e_" + typeId + "_" + ordinal;
    }
}

package org.apache.ignite.spi.communication.tcp.messages;

import java.nio.ByteBuffer;

import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

@IgniteCodeGeneratingFail
public class IgniteHeaderMessage implements SystemMessage {
    /** */
    public static final IgniteHeaderMessage INSTANCE = new IgniteHeaderMessage();

    /** */
    private IgniteHeaderMessage() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        if (buf.remaining() < U.IGNITE_HEADER.length)
            return false;

        buf.put(U.IGNITE_HEADER);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op
    }
}

package org.apache.ignite.spi.communication.tcp.messages;

import java.nio.ByteBuffer;

import org.apache.ignite.plugin.extensions.communication.Message;

public interface SystemMessage extends Message {
    /** */
    public default boolean readFrom(ByteBuffer buff) {
        return readFrom(buff, null);
    }

    /** */
    public default boolean writeTo(ByteBuffer buff) {
        return writeTo(buff, null);
    }
}

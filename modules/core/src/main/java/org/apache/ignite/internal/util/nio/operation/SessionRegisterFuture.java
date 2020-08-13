package org.apache.ignite.internal.util.nio.operation;

import java.nio.channels.SocketChannel;
import java.util.Map;

import org.apache.ignite.internal.util.nio.GridNioKeyAttachment;
import org.apache.ignite.internal.util.nio.GridNioSession;

public class SessionRegisterFuture extends SessionOperationFuture<GridNioSession> implements GridNioKeyAttachment {
    private final SocketChannel channel;
    private final Map<Integer, ?> meta;
    private final boolean accepted;

    public SessionRegisterFuture(NioOperation op, SocketChannel channel, Map<Integer, ?> meta, boolean accepted) {
        super(null, op);
        this.channel = channel;
        this.meta = meta;
        this.accepted = accepted;
    }

    /** {@inheritDoc} */
    @Override public boolean hasSession() {
        return false;
    }

    public SocketChannel channel() {
        return channel;
    }

    public Map<Integer, ?> meta() {
        return meta;
    }

    public boolean accepted() {
        return accepted;
    }
}

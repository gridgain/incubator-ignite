package org.apache.ignite.internal.util.nio.operation;

import java.nio.channels.SocketChannel;

import org.apache.ignite.internal.util.nio.GridSelectorNioSessionImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/** */
public class SessionFinishMoveRequest extends SessionMoveRequest {
    /** */
    @GridToStringExclude
    private final SocketChannel channel;

    /**
     * @param ses Session.
     * @param fromIdx Source worker index.
     * @param toIdx Target worker index.
     * @param channel Moved session socket channel.
     */
    public SessionFinishMoveRequest(GridSelectorNioSessionImpl ses, int fromIdx, int toIdx, SocketChannel channel) {
        super(ses, fromIdx, toIdx);

        this.channel = channel;
    }

    @Override public NioOperation operation() {
        return NioOperation.FINISH_MOVE;
    }

    /**
     * @return Moved session socket channel.
     */
    public SocketChannel channel() {
        return channel;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SessionFinishMoveRequest.class, this, super.toString());
    }
}

package org.apache.ignite.internal.util.nio.operation;

import org.apache.ignite.internal.util.nio.GridSelectorNioSessionImpl;
import org.apache.ignite.internal.util.typedef.internal.S;

/** */
public class SessionStartMoveRequest extends SessionMoveRequest {
    /**
     * @param ses Session.
     * @param fromIdx Source worker index.
     * @param toIdx Target worker index.
     */
    public SessionStartMoveRequest(GridSelectorNioSessionImpl ses, int fromIdx, int toIdx) {
        super(ses, fromIdx, toIdx);
    }

    @Override public NioOperation operation() {
        return NioOperation.START_MOVE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SessionStartMoveRequest.class, this, super.toString());
    }
}

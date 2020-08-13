package org.apache.ignite.internal.util.nio.operation;

import org.apache.ignite.internal.util.nio.GridSelectorNioSessionImpl;
import org.apache.ignite.internal.util.typedef.internal.S;

/** */
public abstract class SessionMoveRequest implements SessionOperationRequest {
    /** */
    private final GridSelectorNioSessionImpl ses;

    /** */
    private final int fromIdx;

    /** */
    private final int toIdx;

    /**
     * @param ses Session.
     * @param fromIdx Source worker index.
     * @param toIdx Target worker index.
     */
    SessionMoveRequest(GridSelectorNioSessionImpl ses, int fromIdx, int toIdx) {
        this.ses = ses;
        this.fromIdx = fromIdx;
        this.toIdx = toIdx;
    }

    /** */
    @Override public GridSelectorNioSessionImpl session() {
        return ses;
    }

    /**
     * @return Source worker index.
     */
    public int fromIndex() {
        return fromIdx;
    }

    /**
     * @return Target worker index.
     */
    public int toIndex() {
        return toIdx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SessionMoveRequest.class, this, super.toString());
    }
}

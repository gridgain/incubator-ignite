package org.apache.ignite.internal.util.nio.operation;

import org.apache.ignite.internal.util.nio.GridNioSession;

public class RecalculateIdleTimeoutRequest implements SessionOperationRequest {
    /** */
    private final GridNioSession ses;

    /** */
    public RecalculateIdleTimeoutRequest(GridNioSession ses) {
        this.ses = ses;
    }

    @Override public GridNioSession session() {
        return ses;
    }

    @Override public NioOperation operation() {
        return NioOperation.RECALCULATE_IDLE_TIMEOUT;
    }
}

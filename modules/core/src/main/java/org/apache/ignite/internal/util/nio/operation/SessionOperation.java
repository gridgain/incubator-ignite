package org.apache.ignite.internal.util.nio.operation;

import org.apache.ignite.internal.util.nio.GridNioSession;

public abstract class SessionOperation implements SessionOperationRequest, Runnable {
    /** */
    private final GridNioSession ses;

    /** */
    protected SessionOperation(GridNioSession ses) {
        this.ses = ses;
    }

    @Override public final GridNioSession session() {
        return ses;
    }

    @Override public final NioOperation operation() {
        return NioOperation.SESSION_OPERATION;
    }
}

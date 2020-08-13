package org.apache.ignite.internal.util.nio.operation;

import org.apache.ignite.internal.util.nio.GridNioSession;

/**
 *
 */
public final class RequireWriteRequest implements SessionOperationRequest {
    /** */
    private final GridNioSession ses;

    /**
     * @param ses Session.
     */
    RequireWriteRequest(GridNioSession ses) {
        this.ses = ses;
    }

    /** {@inheritDoc} */
    @Override public GridNioSession session() {
        return ses;
    }

    /** {@inheritDoc} */
    @Override public NioOperation operation() {
        return NioOperation.REQUIRE_WRITE;
    }
}

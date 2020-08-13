package org.apache.ignite.internal.util.nio.operation;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.nio.GridNioFutureImpl;
import org.apache.ignite.internal.util.nio.GridSelectorNioSessionImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Class for requesting write and session close operations.
 */
public class SessionOperationFuture<R> extends GridNioFutureImpl<R> implements SessionOperationRequest {
    /** Session to perform operation on. */
    @GridToStringExclude
    protected GridSelectorNioSessionImpl ses;

    /** Is it a close request or a write request. */
    protected final NioOperation op;

    /**
     * @param ses Session.
     * @param op Operation.
     */
    public SessionOperationFuture(GridSelectorNioSessionImpl ses, NioOperation op) {
        this(ses, op, null);
    }

    /**
     * @param ses Session.
     * @param op Operation.
     * @param ackC Ack closure.
     */
    public SessionOperationFuture(GridSelectorNioSessionImpl ses, NioOperation op,
        IgniteInClosure<IgniteException> ackC) {
        this.ses = ses;
        this.op = op;
    }

    /** {@inheritDoc} */
    @Override public NioOperation operation() {
        return op;
    }

    /** {@inheritDoc} */
    @Override public GridSelectorNioSessionImpl session() {
        return ses;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SessionOperationFuture.class, this);
    }
}

package org.apache.ignite.internal.util.nio.operation;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.nio.GridSelectorNioSessionImpl;

public class SessionCloseFuture extends SessionOperationFuture<Boolean> {
    private final IgniteCheckedException cause;

    public SessionCloseFuture(GridSelectorNioSessionImpl ses, IgniteCheckedException cause) {
        super(ses, NioOperation.CLOSE);
        this.cause = cause;
    }

    public IgniteCheckedException cause() {
        return cause;
    }
}

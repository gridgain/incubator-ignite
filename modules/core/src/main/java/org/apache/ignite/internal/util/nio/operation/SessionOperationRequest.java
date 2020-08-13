package org.apache.ignite.internal.util.nio.operation;

import org.apache.ignite.internal.util.nio.GridNioSession;

/**
 *
 */
public interface SessionOperationRequest {
    /**
     * @return Session.
     */
    GridNioSession session();

    /**
     * @return Requested change operation.
     */
    NioOperation operation();
}

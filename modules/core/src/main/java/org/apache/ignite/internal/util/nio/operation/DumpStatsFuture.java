package org.apache.ignite.internal.util.nio.operation;

import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.lang.IgnitePredicate;

public class DumpStatsFuture extends SessionOperationFuture<String> {
    /** */
    private final IgnitePredicate<GridNioSession> predicate;

    /**
     * @param predicate Session predicate.
     */
    public DumpStatsFuture(IgnitePredicate<GridNioSession> predicate) {
        super(null, NioOperation.DUMP_STATS);

        this.predicate = predicate;
    }

    /**
     * @return Session predicate.
     */
    public IgnitePredicate<GridNioSession> predicate() {
        return predicate;
    }
}

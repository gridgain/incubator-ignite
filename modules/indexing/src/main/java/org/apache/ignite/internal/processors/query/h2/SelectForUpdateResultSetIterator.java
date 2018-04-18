package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.LockingOperationSourceIterator;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapterEx;
import org.h2.value.Value;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * Simple iterator over result set returning key and value located at the end of each row.
 */
class SelectForUpdateResultSetIterator extends GridCloseableIteratorAdapterEx<Object[]>
    implements LockingOperationSourceIterator<Object[]> {
    /** Iterator over page. */
    private final ResultSet rs;

    /** Columns count in result set. */
    private final int colsCnt;

    /** */
    private boolean hasNext;

    /**
     * @param rs SELECT results page iterator.
     */
    SelectForUpdateResultSetIterator(ResultSet rs) {
        this.rs = rs;

        try {
            colsCnt = rs.getMetaData().getColumnCount();

            hasNext = rs.next();
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected Object[] onNext() {
        Object key;

        Object val;

        try {
            key = rs.getObject(colsCnt - 1);

            val = rs.getObject(colsCnt);

            hasNext = rs.next();
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }

        return new Object[] { key, val };
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() {
        return hasNext;
    }

    /** {@inheritDoc} */
    @Override public void beforeDetach() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridCacheOperation operation() {
        return GridCacheOperation.SELECT;
    }
}

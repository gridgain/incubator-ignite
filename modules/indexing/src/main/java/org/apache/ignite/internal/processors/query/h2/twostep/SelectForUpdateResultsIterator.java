package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Iterator;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.query.LockingOperationSourceIterator;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapterEx;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.h2.value.Value;

/**
 * Simple iterator over SELECT results page returning key and value located at the end of the row.
 */
class SelectForUpdateResultsIterator extends GridCloseableIteratorAdapterEx<IgnitePair<Object>>
    implements LockingOperationSourceIterator<IgnitePair<Object>> {
    /** Iterator over page. */
    private final Iterator<Value[]> rowsIt;

    /**
     * @param rows SELECT results page iterator.
     */
    SelectForUpdateResultsIterator(Iterator<Value[]> rows) {
        this.rowsIt = rows;
    }

    /** {@inheritDoc} */
    @Override protected IgnitePair<Object> onNext() {
        Value[] row = rowsIt.next();

        int keyIdx = row.length - 2;

        return new IgnitePair<>(row[keyIdx].getObject(), row[keyIdx + 1].getObject());
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() {
        return rowsIt.hasNext();
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

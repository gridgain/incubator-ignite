package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Iterator;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.query.LockingOperationSourceIterator;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapterEx;
import org.h2.value.Value;

/**
 * Simple iterator over SELECT results page returning key located at the end of the row.
 */
class SelectForUpdatePageIterator extends GridCloseableIteratorAdapterEx<Object>
    implements LockingOperationSourceIterator<Object> {
    /** Iterator over page. */
    private final Iterator<Value[]> rowsIt;

    /**
     * @param rows SELECT results page iterator.
     */
    SelectForUpdatePageIterator(Iterator<Value[]> rows) {
        this.rowsIt = rows;
    }

    /** {@inheritDoc} */
    @Override protected Object onNext() {
        Value[] row = rowsIt.next();

        int keyIdx = row.length - 1;

        return row[keyIdx].getObject();
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

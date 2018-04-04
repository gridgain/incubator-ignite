package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.query.LockingOperationSourceIterator;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapterEx;
import org.apache.ignite.lang.IgniteBiTuple;
import org.h2.value.Value;

import java.util.Iterator;

/**
 * Simple iterator over SELECT results page returning key and value located at the end of the row.
 */
class SelectForUpdateResultsIterator extends GridCloseableIteratorAdapterEx<IgniteBiTuple<Object, Object>>
    implements LockingOperationSourceIterator<IgniteBiTuple<Object, Object>> {
    /** Iterator over page. */
    private final Iterator<Value[]> rowsIt;

    /**
     * @param rows SELECT results page iterable.
     */
    SelectForUpdateResultsIterator(Iterable<Value[]> rows) {
        this.rowsIt = rows.iterator();
    }

    /** {@inheritDoc} */
    @Override protected IgniteBiTuple<Object, Object> onNext() throws IgniteCheckedException {
        Value[] row = rowsIt.next();

        int keyIdx = row.length - 2;

        return new IgniteBiTuple<>(row[keyIdx].getObject(), row[keyIdx + 1].getObject());
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() throws IgniteCheckedException {
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

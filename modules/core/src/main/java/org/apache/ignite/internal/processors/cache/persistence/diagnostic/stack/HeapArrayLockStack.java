package org.apache.ignite.internal.processors.cache.persistence.diagnostic.stack;

import static java.util.Arrays.copyOf;

public class HeapArrayLockStack extends LockStack {
    private final long[] pageIdLocksStack;

    public HeapArrayLockStack(String name, int capacity) {
        super(name, capacity);

        this.pageIdLocksStack = new long[capacity];
    }

    @Override protected long getByIndex(int idx) {
        return pageIdLocksStack[idx];
    }

    @Override protected void setByIndex(int idx, long val) {
        pageIdLocksStack[idx] = val;
    }

    /** {@inheritDoc} */
    @Override public LocksStackSnapshot dump0() {
        long[] stack = copyOf(pageIdLocksStack, pageIdLocksStack.length);

        return new LocksStackSnapshot(
            name,
            System.currentTimeMillis(),
            headIdx,
            stack,
            nextOp,
            nextOpStructureId,
            nextOpPageId
        );
    }
}

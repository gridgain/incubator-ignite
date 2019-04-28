package org.apache.ignite.internal.processors.cache.persistence.diagnostic.log;

import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.GridUnsafe;

public class OffHeapLockLog extends LockLog {
    private static final int LOG_CAPACITY = 128;
    private static final int LOG_SIZE = (LOG_CAPACITY * 8) * 2;

    private final long ptr;

    public OffHeapLockLog(String name) {
        super(name);

        this.ptr = allocate(LOG_SIZE);
    }

    @Override public int capacity() {
        return LOG_CAPACITY;
    }

    @Override protected long getByIndex(int idx) {
        return GridUnsafe.getLong(ptr + offset(idx));
    }

    @Override protected void setByIndex(int idx, long val) {
        GridUnsafe.putLong(ptr + offset(idx), val);
    }

    private long offset(long headIdx) {
        return headIdx * 8;
    }

    private long allocate(int size) {
        long ptr = GridUnsafe.allocateMemory(size);

        GridUnsafe.setMemory(ptr, LOG_SIZE, (byte)0);

        return ptr;
    }

    @Override public LockLogSnapshot dump0() {
        return new LockLogSnapshot(
            name,
            System.currentTimeMillis(),
            headIdx,
            toList(),
            nextOp,
            nextOpStructureId,
            nextOpPageId
        );
    }

    @Override protected List<LockLogSnapshot.LogEntry> toList() {
        List<LockLogSnapshot.LogEntry> lockLog = new ArrayList<>(LOG_CAPACITY);

        for (int i = 0; i < headIdx; i += 2) {
            long metaOnLock = getByIndex(i + 1);

            assert metaOnLock != 0;

            int idx = ((int)(metaOnLock >> 32) & LOCK_IDX_MASK) >> OP_OFFSET;

            assert idx >= 0;

            long pageId = getByIndex(i);

            int op = (int)((metaOnLock >> 32) & LOCK_OP_MASK);
            int structureId = (int)(metaOnLock);

            lockLog.add(new LockLogSnapshot.LogEntry(pageId, structureId, op, idx));
        }

        return lockLog;
    }
}

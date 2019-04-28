package org.apache.ignite.internal.processors.cache.persistence.diagnostic.log;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.GridUnsafe;

public class OffHeapLockLog extends LockLog {
    private final int logSize;

    private final long ptr;

    public OffHeapLockLog(String name, int capacity) {
        super(name, capacity);

        this.logSize = (capacity * 8) * 2;
        this.ptr = allocate(logSize);
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

        GridUnsafe.setMemory(ptr, logSize, (byte)0);

        return ptr;
    }

    @Override public LockLogSnapshot snapshot() {
        return new LockLogSnapshot(
            name,
            System.currentTimeMillis(),
            headIdx / 2,
            toList(),
            nextOp,
            nextOpStructureId,
            nextOpPageId
        );
    }

    @Override protected List<LockLogSnapshot.LogEntry> toList() {
        List<LockLogSnapshot.LogEntry> lockLog = new ArrayList<>(capacity);

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

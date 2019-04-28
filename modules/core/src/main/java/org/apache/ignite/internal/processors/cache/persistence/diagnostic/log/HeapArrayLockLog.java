package org.apache.ignite.internal.processors.cache.persistence.diagnostic.log;

import java.util.ArrayList;
import java.util.List;

public class HeapArrayLockLog extends LockLog {
    private static final int LOG_SIZE = 128;

    private final long[] pageIdsLockLog = new long[LOG_SIZE * 2];

    public HeapArrayLockLog(String name) {
        super(name);
    }

    @Override public int capacity() {
        return LOG_SIZE;
    }

    @Override protected long getByIndex(int idx) {
        return pageIdsLockLog[idx];
    }

    @Override protected void setByIndex(int idx, long val) {
        pageIdsLockLog[idx] = val;
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
        List<LockLogSnapshot.LogEntry> lockLog = new ArrayList<>(LOG_SIZE);

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

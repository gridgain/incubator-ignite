package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log;

import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker;

public abstract class LockLog extends PageLockTracker<LockLogSnapshot> {
    public static final int OP_OFFSET = 16;
    public static final int LOCK_IDX_MASK = 0xFFFF0000;
    public static final int LOCK_OP_MASK = 0x000000000000FF;

    protected int headIdx;

    protected int holdedLockCnt;

    protected int nextOp;
    protected int nextOpStructureId;
    protected long nextOpPageId;

    protected LockLog(String name, int capacity) {
        super(name, capacity);
    }

    @Override protected void onBeforeWriteLock0(int structureId, long pageId, long page) {
        this.nextOp = BEFORE_WRITE_LOCK;
        this.nextOpStructureId = structureId;
        this.nextOpPageId = pageId;
    }

    @Override protected void onWriteLock0(int structureId, long pageId, long page, long pageAddr) {
        log(structureId, pageId, WRITE_LOCK);
    }

    @Override protected void onWriteUnlock0(int structureId, long pageId, long page, long pageAddr) {
        log(structureId, pageId, WRITE_UNLOCK);
    }

    @Override protected void onBeforeReadLock0(int structureId, long pageId, long page) {
        this.nextOp = BEFORE_READ_LOCK;
        this.nextOpStructureId = structureId;
        this.nextOpPageId = pageId;
    }

    @Override protected void onReadLock0(int structureId, long pageId, long page, long pageAddr) {
        log(structureId, pageId, READ_LOCK);
    }

    @Override protected void onReadUnlock0(int structureId, long pageId, long page, long pageAddr) {
        log(structureId, pageId, READ_UNLOCK);
    }

    private void log(int structureId, long pageId, int op) {
        assert pageId > 0;

        if ((headIdx + 2) / 2 > capacity()) {
            invalid("Log overflow, size:" + capacity() +
                ", headIdx=" + headIdx + " " + argsToString(structureId, pageId, op));

            return;
        }

        long pageId0 = getByIndex(headIdx);

        if (pageId0 != 0L && pageId0 != pageId) {
            invalid("Head should be empty, headIdx=" + headIdx + " " +
                argsToString(structureId, pageId, op));

            return;
        }

        setByIndex(headIdx, pageId);

        if (READ_LOCK == op || WRITE_LOCK == op)
            holdedLockCnt++;

        if (READ_UNLOCK == op || WRITE_UNLOCK == op)
            holdedLockCnt--;

        int curIdx = holdedLockCnt << OP_OFFSET & LOCK_IDX_MASK;

        long meta = meta(structureId, curIdx | op);

        setByIndex(headIdx + 1, meta);

        if (BEFORE_READ_LOCK == op || BEFORE_WRITE_LOCK == op)
            return;

        headIdx += 2;

        if (holdedLockCnt == 0)
            reset();

        if (op != BEFORE_READ_LOCK && op != BEFORE_WRITE_LOCK &&
            nextOpPageId == pageId && nextOpStructureId == structureId) {
            nextOpStructureId = 0;
            nextOpPageId = 0;
            nextOp = 0;
        }
    }

    private void reset() {
        for (int i = 0; i < headIdx; i++)
            setByIndex(0, 0);

        headIdx = 0;
    }

    private long meta(int structureId, int flags) {
        long major = ((long)flags) << 32;

        long minor = (long)structureId;

        return major | minor;
    }

    protected abstract List<LockLogSnapshot.LogEntry> toList();
}

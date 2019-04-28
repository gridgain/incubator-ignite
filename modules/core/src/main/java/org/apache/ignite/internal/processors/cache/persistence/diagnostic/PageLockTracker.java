package org.apache.ignite.internal.processors.cache.persistence.diagnostic;

import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.lang.IgniteFuture;

import static org.apache.ignite.internal.pagemem.PageIdUtils.flag;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.util.IgniteUtils.hexInt;
import static org.apache.ignite.internal.util.IgniteUtils.hexLong;

public abstract class PageLockTracker<T extends Dump> implements PageLockListener, DumpSupported<T> {
    public static final int READ_LOCK = 1;
    public static final int READ_UNLOCK = 2;
    public static final int WRITE_LOCK = 3;
    public static final int WRITE_UNLOCK = 4;
    public static final int BEFORE_READ_LOCK = 5;
    public static final int BEFORE_WRITE_LOCK = 6;

    protected final String name;

    protected final int capacity;

    private volatile boolean dump;

    private volatile boolean locked;

    private volatile InvalidContext<T> invalidCtx;

    protected PageLockTracker(String name, int capacity) {
        this.name = name;
        this.capacity = capacity;
    }

    @Override public void onBeforeWriteLock(int structureId, long pageId, long page) {
        if (isInvalid())
            return;

        lock();

        try {
            onBeforeWriteLock0(structureId, pageId, page);
        }
        finally {
            unLock();
        }
    }

    @Override public void onWriteLock(int structureId, long pageId, long page, long pageAddr) {
        if (isInvalid())
            return;

        lock();

        try {
            onWriteLock0(structureId, pageId, page, pageAddr);
        }
        finally {
            unLock();
        }
    }

    @Override public void onWriteUnlock(int structureId, long pageId, long page, long pageAddr) {
        if (isInvalid())
            return;

        lock();

        try {
            onWriteUnlock0(structureId, pageId, page, pageAddr);
        }
        finally {
            unLock();
        }
    }

    @Override public void onBeforeReadLock(int structureId, long pageId, long page) {
        if (isInvalid())
            return;

        lock();

        try {
            onBeforeReadLock0(structureId, pageId, page);
        }
        finally {
            unLock();
        }
    }

    @Override public void onReadLock(int structureId, long pageId, long page, long pageAddr) {
        if (isInvalid())
            return;

        lock();

        try {
            onReadLock0(structureId, pageId, page, pageAddr);
        }
        finally {
            unLock();
        }
    }

    @Override public void onReadUnlock(int structureId, long pageId, long page, long pageAddr) {
        if (isInvalid())
            return;

        lock();

        try {
            onReadUnlock0(structureId, pageId, page, pageAddr);
        }
        finally {
            unLock();
        }
    }

    protected abstract void onBeforeWriteLock0(int structureId, long pageId, long page);

    protected abstract void onWriteLock0(int structureId, long pageId, long page, long pageAddr);

    protected abstract void onWriteUnlock0(int structureId, long pageId, long page, long pageAddr);

    protected abstract void onBeforeReadLock0(int structureId, long pageId, long page);

    protected abstract void onReadLock0(int structureId, long pageId, long page, long pageAddr);

    protected abstract void onReadUnlock0(int structureId, long pageId, long page, long pageAddr);

    public boolean isInvalid() {
        return invalidCtx != null;
    }

    public InvalidContext<T> invalidContext() {
        return invalidCtx;
    }

    public int capacity() {
        return capacity;
    }

    protected abstract long getByIndex(int idx);

    protected abstract void setByIndex(int idx, long val);

    protected void invalid(String msg) {
        T dump = dump0();

        invalidCtx = new InvalidContext<>(msg, dump);
    }

    private void lock() {
        while (!lock0()) {
            // Busy wait.
        }
    }

    private boolean lock0() {
        awaitDump();

        locked = true;
        if (dump) {
            locked = false;

            return false;
        }

        return true;
    }

    private void unLock() {
        locked = false;
    }

    private void awaitDump() {
        while (dump) {
            // Busy wait.
        }
    }

    private void awaitLocks() {
        while (locked) {
            // Busy wait.
        }
    }

    protected abstract T dump0();

    @Override public synchronized T dump() {
        dump = true;

        awaitLocks();

        T dump0 = dump0();

        dump = false;

        return dump0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Dump dump = dump();

        return dump.toString();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<T> dumpSync() {
        //TODO
        throw new UnsupportedOperationException();
    }

    public static String argsToString(int structureId, long pageId, int flags) {
        return "[structureId=" + structureId + ", pageId" + pageIdToString(pageId) + "]";
    }

    public static String pageIdToString(long pageId) {
        return "pageId=" + pageId
            + " [pageIdxHex=" + hexLong(pageId)
            + ", partId=" + pageId(pageId) + ", pageIdx=" + pageIndex(pageId)
            + ", flags=" + hexInt(flag(pageId)) + "]";
    }
}

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.stack;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.PageLockTracker;

public abstract class LockStack extends PageLockTracker<LockStackSnapshot> {
    protected int headIdx;

    protected int nextOp;
    protected int nextOpStructureId;
    protected long nextOpPageId;

    protected LockStack(String name, int capacity) {
        super(name, capacity);
    }

    @Override protected void onBeforeWriteLock0(int structureId, long pageId, long page) {
        nextOpPageId = pageId;
        nextOp = BEFORE_WRITE_LOCK;
    }

    @Override protected void onWriteLock0(int structureId, long pageId, long page, long pageAddr) {
        push(structureId, pageId, WRITE_LOCK);
    }

    @Override protected void onWriteUnlock0(int structureId, long pageId, long page, long pageAddr) {
        pop(structureId, pageId, WRITE_UNLOCK);
    }

    @Override protected void onBeforeReadLock0(int structureId, long pageId, long page) {
        nextOpPageId = pageId;
        nextOp = BEFORE_READ_LOCK;
    }

    @Override protected void onReadLock0(int structureId, long pageId, long page, long pageAddr) {
        push(structureId, pageId, READ_LOCK);
    }

    @Override protected void onReadUnlock0(int structureId, long pageId, long page, long pageAddr) {
        pop(structureId, pageId, READ_UNLOCK);
    }

    private void push(int structureId, long pageId, int flags) {
        reset(flags);

        if (headIdx + 1 > capacity()) {
            invalid("Stack overflow, size=" + capacity() +
                ", headIdx=" + headIdx + " " + argsToString(structureId, pageId, flags));

            return;
        }

        long pageId0 = getByIndex(headIdx);

        if (pageId0 != 0L) {
            invalid("Head element should be empty, headIdx=" + headIdx +
                ", pageIdOnHead=" + pageId0 + " " + argsToString(structureId, pageId, flags));

            return;
        }

        setByIndex(headIdx, pageId);

        headIdx++;
    }

    private void reset(int flags) {
        if (flags != READ_LOCK || flags != WRITE_LOCK) {
            //TODO asserts
        }

        nextOpPageId = 0;
        nextOp = 0;
        nextOpStructureId = 0;
    }

    private void pop(int structureId, long pageId, int flags) {
        reset(flags);

        if (headIdx > 1) {
            int last = headIdx - 1;

            long val = getByIndex(last);

            if (val == pageId) {
                setByIndex(last, 0);

                //Reset head to the first not empty element.
                do {
                    headIdx--;
                }
                while (headIdx - 1 >= 0 && getByIndex(headIdx - 1) == 0);
            }
            else {
                for (int idx = last - 1; idx >= 0; idx--) {
                    if (getByIndex(idx) == pageId) {
                        setByIndex(idx, 0);

                        return;
                    }
                }

                invalid("Can not find pageId in stack, headIdx=" + headIdx + " "
                    + argsToString(structureId, pageId, flags));
            }
        }
        else {
            if (headIdx < 0) {
                invalid("HeadIdx can not be less, headIdx="
                    + headIdx + ", " + argsToString(structureId, pageId, flags));

                return;
            }

            long val = getByIndex(0);

            if (val == 0) {
                invalid("Stack is empty, can not pop elemnt" + argsToString(structureId, pageId, flags));

                return;
            }

            if (val == pageId) {
                setByIndex(0, 0);

                headIdx = 0;
            }
            else
                invalid("Can not find pageId in stack, headIdx=" + headIdx + " "
                    + argsToString(structureId, pageId, flags));
        }
    }
}

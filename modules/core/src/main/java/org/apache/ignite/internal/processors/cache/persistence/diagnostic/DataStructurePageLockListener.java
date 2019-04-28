package org.apache.ignite.internal.processors.cache.persistence.diagnostic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.stack.HeapArrayLockStack;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.lang.IgniteFuture;

public class DataStructurePageLockListener implements PageLockListener, DumpSupported<ThreadDumpLocks> {
    public static final DataStructurePageLockListener LOCK_TRACKER = new DataStructurePageLockListener();

    private final Map<Long, PageLockTracker> threadStacks = new ConcurrentHashMap<>();
    private final Map<Long, String> idToThreadName = new ConcurrentHashMap<>();

    private int idx;

    private final Map<String, Integer> structureNameToId = new HashMap<>();

    /** */
    private final ThreadLocal<PageLockTracker> lockTracker = ThreadLocal.withInitial(() -> {
        Thread thread = Thread.currentThread();

        String threadName = thread.getName();
        long threadId = thread.getId();

        PageLockTracker stack = LockTracerFactory.create(threadName + "[" + threadId + "]");

        threadStacks.put(threadId, stack);

        idToThreadName.put(threadId, threadName);

        return stack;
    });

    private DataStructurePageLockListener() {

    }

    public synchronized PageLockListener registrateStructure(String structureName) {
        Integer idx = structureNameToId.get(structureName);

        if (idx == null)
            structureNameToId.put(structureName, idx = (++this.idx));

        return new PageLockListenerIndexAdapter(idx, this);
    }

    @Override public void onBeforeWriteLock(int structureId, long pageId, long page) {
        lockTracker.get().onBeforeWriteLock(structureId, pageId, page);
    }

    @Override public void onWriteLock(int structureId, long pageId, long page, long pageAddr) {
        lockTracker.get().onWriteLock(structureId, pageId, page, pageAddr);
    }

    @Override public void onWriteUnlock(int structureId, long pageId, long page, long pageAddr) {
        lockTracker.get().onWriteUnlock(structureId, pageId, page, pageAddr);
    }

    @Override public void onBeforeReadLock(int structureId, long pageId, long page) {
        lockTracker.get().onBeforeReadLock(structureId, pageId, page);
    }

    @Override public void onReadLock(int structureId, long pageId, long page, long pageAddr) {
        lockTracker.get().onReadLock(structureId, pageId, page, pageAddr);
    }

    @Override public void onReadUnlock(int structureId, long pageId, long page, long pageAddr) {
        lockTracker.get().onReadUnlock(structureId, pageId, page, pageAddr);
    }

    @Override public synchronized ThreadDumpLocks dump() {
        Map<Long, Dump> dumps = threadStacks.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().dump()
            ));

        Map<Integer, String> idToStrcutureName =
            structureNameToId.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getValue,
                    Map.Entry::getKey
                ));

        return new ThreadDumpLocks(idToStrcutureName, idToThreadName, dumps);
    }

    @Override public IgniteFuture<ThreadDumpLocks> dumpSync() {
        throw new UnsupportedOperationException();
    }
}

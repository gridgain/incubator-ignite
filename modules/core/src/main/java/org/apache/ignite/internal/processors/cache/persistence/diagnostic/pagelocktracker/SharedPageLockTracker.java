package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.lang.IgniteFuture;

public class SharedPageLockTracker implements PageLockListener, DumpSupported<ThreadDumpLocks> {
    public static final SharedPageLockTracker LOCK_TRACKER = new SharedPageLockTracker();

    private final Map<Long, PageLockTracker> threadStacks = new ConcurrentHashMap<>();
    private final Map<Long, String> idToThreadName = new ConcurrentHashMap<>();

    private int idGen;

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

    private SharedPageLockTracker() {

    }

    public synchronized PageLockListener registrateStructure(String structureName) {
        Integer id = structureNameToId.get(structureName);

        if (id == null)
            structureNameToId.put(structureName, id = (++idGen));

        return new PageLockListenerIndexAdapter(id, this);
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

    @Override public boolean acquireSafePoint() {
        throw new UnsupportedOperationException();
    }

    @Override public boolean releaseSafePoint() {
        throw new UnsupportedOperationException();
    }

    @Override public synchronized ThreadDumpLocks dump() {
        Collection<PageLockTracker> trackers = threadStacks.values();

        Map<PageLockTracker, Boolean> needReleaseMap = new HashMap<>(threadStacks.size());

        for (PageLockTracker tracker : trackers) {
            boolean acquired = tracker.acquireSafePoint();

            needReleaseMap.put(tracker, acquired);
        }

        Map<Long, Dump> dumps = threadStacks.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> {
                    PageLockTracker tracker = e.getValue();

                    Dump dump = tracker.dump();

                    Boolean needRelease = needReleaseMap.get(tracker);

                    if (needRelease)
                        tracker.releaseSafePoint();

                    return dump;
                }
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

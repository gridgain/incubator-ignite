package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.lang.IgniteFuture;

public class SharedPageLockTracker implements PageLockListener, DumpSupported<ThreadDumpLocks> {
    private final Map<Long, PageLockTracker> threadStacks = new HashMap<>();
    private final Map<Long, String> idToThreadName = new HashMap<>();

    private int idGen;

    private final Map<String, Integer> structureNameToId = new HashMap<>();

    /** */
    private final ThreadLocal<PageLockTracker> lockTracker = ThreadLocal.withInitial(() -> {
        Thread thread = Thread.currentThread();

        String threadName = thread.getName();
        long threadId = thread.getId();

        PageLockTracker tracker = LockTracerFactory.create(threadName + "[" + threadId + "]");

        synchronized (this) {
            threadStacks.put(threadId, tracker);

            idToThreadName.put(threadId, threadName);
        }

        return tracker;
    });

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

    @Override public synchronized ThreadDumpLocks dump() {
        Collection<PageLockTracker> trackers = threadStacks.values();

        for (PageLockTracker tracker : trackers) {
            boolean acquired = tracker.acquireSafePoint();

            //TODO
            assert acquired;
        }

        Map<Long, Dump> dumps = new HashMap<>();
        Map<Long, InvalidContext<Dump>> invalidThreads = new HashMap<>();

        for (Map.Entry<Long, PageLockTracker> entry : threadStacks.entrySet()) {
            Long threadId = entry.getKey();

            PageLockTracker<Dump> tracker = entry.getValue();

            try {
                Dump dump = tracker.dump();

                if (tracker.isInvalid()) {
                    InvalidContext<Dump> invalidContext = tracker.invalidContext();

                    invalidThreads.put(threadId, invalidContext);
                }
                else
                    dumps.put(threadId, dump);
            }
            finally {
                tracker.releaseSafePoint();
            }
        }

        Map<Integer, String> idToStrcutureName0 =
            structureNameToId.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getValue,
                    Map.Entry::getKey
                ));

        Map<Long, String> idToThreadName0 = new HashMap<>(idToThreadName);

        return new ThreadDumpLocks(idToStrcutureName0, idToThreadName0, dumps, invalidThreads);
    }

    @Override public IgniteFuture<ThreadDumpLocks> dumpSync() {
        throw new UnsupportedOperationException();
    }

    @Override public boolean acquireSafePoint() {
        throw new UnsupportedOperationException();
    }

    @Override public boolean releaseSafePoint() {
        throw new UnsupportedOperationException();
    }
}

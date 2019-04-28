package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

public interface PageLockMXBean extends PageLockManager {
    void enableTracking();

    void disableTracking();

    boolean isTracingEnable();

    String dumpLocks();

    void dumpLocksToLog();

    void dumpLocksToFile();

    void dumpLocksToFile(String path);
}

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

public interface PageLockMXBean {
    void enableTracking();

    void disableTracking();

    boolean isTracingEnable();

    String dumpLocks();

    void dumpLocksToLog();

    String dumpLocksToFile();

    String dumpLocksToFile(String path);
}

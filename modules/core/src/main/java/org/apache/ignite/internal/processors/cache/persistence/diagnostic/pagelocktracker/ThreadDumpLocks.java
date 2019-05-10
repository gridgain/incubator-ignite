package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.util.Map;

public class ThreadDumpLocks implements Dump {
    public final Map<Integer, String> structureIdToStrcutureName;

    public final Map<Long, String> threadIdToThreadName;

    public final Map<Long, Dump> threadIdToDump;

    private final Map<Long, InvalidContext<Dump>> invalidThreads;

    public ThreadDumpLocks(
        Map<Integer, String> structureIdToStrcutureName,
        Map<Long, String> threadIdToThreadName,
        Map<Long, Dump> threadIdToDump,
        Map<Long, InvalidContext<Dump>> invalidThreads
    ) {
        this.structureIdToStrcutureName = structureIdToStrcutureName;
        this.threadIdToThreadName = threadIdToThreadName;
        this.threadIdToDump = threadIdToDump;
        this.invalidThreads = invalidThreads;
    }
}

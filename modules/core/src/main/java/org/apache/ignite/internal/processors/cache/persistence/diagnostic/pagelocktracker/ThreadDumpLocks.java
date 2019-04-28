package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.util.Map;

public class ThreadDumpLocks implements Dump {

    public final Map<Integer, String> idToStrcutureName;

    public final Map<Long, String> idToThreadName;

    public final Map<Long, Dump> dumps;

    public ThreadDumpLocks(
        Map<Integer, String> idToStrcutureName,
        Map<Long, String> idToThreadName,
        Map<Long, Dump> dumps
    ) {
        this.idToStrcutureName = idToStrcutureName;
        this.idToThreadName = idToThreadName;
        this.dumps = dumps;
    }
}

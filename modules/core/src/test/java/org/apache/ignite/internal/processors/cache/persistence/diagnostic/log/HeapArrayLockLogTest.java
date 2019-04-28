package org.apache.ignite.internal.processors.cache.persistence.diagnostic.log;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.LockTracerFactory;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.PageLockLogTest;

import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.LockTracerFactory.HEAP_LOG;

public class HeapArrayLockLogTest extends PageLockLogTest {

    @Override protected LockLog createLogStackTracer(String name) {
        return (LockLog)LockTracerFactory.create(HEAP_LOG, name);
    }
}
package org.apache.ignite.internal.processors.cache.persistence.diagnostic.stack;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.LockTracerFactory;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.PageLockStackTest;

import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.LockTracerFactory.HEAP_STACK;

public class HeapArrayLockStackTest extends PageLockStackTest {
    @Override protected LockStack createLockStackTracer(String name) {
        return (LockStack)LockTracerFactory.create(HEAP_STACK, name);
    }
}

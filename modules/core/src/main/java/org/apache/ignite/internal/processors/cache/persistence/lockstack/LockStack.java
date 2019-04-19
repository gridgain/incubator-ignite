package org.apache.ignite.internal.processors.cache.persistence.lockstack;

import org.apache.ignite.IgniteException;

public interface LockStack {
    void push(int cacheId, long pageId, int flags);

    void pop(int cacheId, long pageId, int flags);

    int poistionIdx();

    int capacity();

    int READ = 0x0000_0001;
    int WRITE = 0x0000_0002;
}

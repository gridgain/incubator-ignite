/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.schema;

import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Index operation cancellation token.
 */
public class SchemaIndexOperationCancellationToken {
    /** Cancel flag. */
    private final AtomicBoolean flag = new AtomicBoolean();

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Get cancel state.
     *
     * @return {@code True} if cancelled.
     */
    public boolean isCancelled() {
        return flag.get();
    }

    public void getLock() {
        lock.readLock().lock();
    }

    public void unLock() {
        lock.readLock().unlock();
    }

    /**
     * Do cancel.
     *
     * @return {@code True} if cancel flag was set by this call.
     */
    public boolean cancel() {
        lock.writeLock().lock();
        try {
            return flag.compareAndSet(false, true);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaIndexOperationCancellationToken.class, this);
    }
}

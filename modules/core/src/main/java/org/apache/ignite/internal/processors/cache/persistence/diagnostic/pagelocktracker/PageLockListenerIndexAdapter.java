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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;

public class PageLockListenerIndexAdapter implements PageLockListener {
    private final int idx;

    private final PageLockListener delegate;

    public PageLockListenerIndexAdapter(int idx, PageLockListener delegate) {
        this.idx = idx;
        this.delegate = delegate;
    }

    @Override public void onBeforeWriteLock(int cacheId, long pageId, long page) {
        delegate.onBeforeWriteLock(idx, pageId, page);
    }

    @Override public void onWriteLock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onWriteLock(idx, pageId, page, pageAddr);
    }

    @Override public void onWriteUnlock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onWriteUnlock(idx, pageId, page, pageAddr);
    }

    @Override public void onBeforeReadLock(int cacheId, long pageId, long page) {
        delegate.onBeforeReadLock(idx, pageId, page);
    }

    @Override public void onReadLock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onReadLock(idx, pageId, page, pageAddr);
    }

    @Override public void onReadUnlock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onReadUnlock(idx, pageId, page, pageAddr);
    }
}

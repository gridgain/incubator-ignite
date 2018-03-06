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

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.internal.pagemem.PageMemory;

/**
 * Page locator for accessing pages both in memory and persistence storage.
 */
public class VacuumPageLocator {

    // TODO replace with diapason
    /** Page ID. */
    private final long pageId;

    /** Cache group ID (for persistence storage) */
    private final int grpId;

    /** Page memory (for in-memory storage) */
    private final PageMemory pageMem;

    /**
     * @param pageId Page id.
     * @param grpId Group id. Should be {@code 0} for in-memory data regions and not {@code 0} for persistence.
     * @param pageMem Page memory. Should be not {@code null} for in-memory storage and {@code null} for persistence.
     */
    public VacuumPageLocator(long pageId, int grpId, PageMemory pageMem) {
        assert grpId == 0 ^ pageMem == null;
        this.pageId = pageId;
        this.grpId = grpId;
        this.pageMem = pageMem;
    }

    /**
     * @return {@code True} if page belongs to in-memory only data region.
     */
    public boolean inMemory() {
        return pageMem != null;
    }

    /**
     * @return Page id.
     */
    public long pageId() {
        return pageId;
    }

    /**
     * @return Group id.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * @return Page memory.
     */
    public PageMemory pageMem() {
        return pageMem;
    }

    @Override public String toString() {
        return "VacuumPageLocator{" +
            "pageId=" + pageId +
            ", grpId=" + grpId +
            ", pageMem=" + pageMem +
            '}';
    }
}

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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log;

import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.Dump;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.DumpProcessor;

import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

public class LockLogSnapshot implements Dump {
    public final String name;

    public final long time;

    public final int headIdx;

    public final List<LogEntry> locklog;

    public final int nextOp;
    public final int nextOpStructureId;
    public final long nextOpPageId;

    public LockLogSnapshot(
        String name,
        long time,
        int headIdx,
        List<LogEntry> locklog,
        int nextOp,
        int nextOpStructureId,
        long nextOpPageId
    ) {
        this.name = name;
        this.time = time;
        this.headIdx = headIdx;
        this.locklog = locklog;
        this.nextOp = nextOp;
        this.nextOpStructureId = nextOpStructureId;
        this.nextOpPageId = nextOpPageId;
    }

    public static class LogEntry {
        public final long pageId;

        public final int structureId;

        public final int operation;

        public final int holdedLocks;

        public LogEntry(long pageId, int structureId, int operation, int holdedLock) {
            this.pageId = pageId;
            this.structureId = structureId;
            this.operation = operation;
            this.holdedLocks = holdedLock;
        }
    }

    @Override public void apply(DumpProcessor dumpProcessor) {
        dumpProcessor.processDump(this);
    }
}
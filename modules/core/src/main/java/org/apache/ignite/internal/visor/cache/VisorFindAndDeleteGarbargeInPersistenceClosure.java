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
package org.apache.ignite.internal.visor.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

/**
 *
 */
public class VisorFindAndDeleteGarbargeInPersistenceClosure implements IgniteCallable<VisorFindAndDeleteGarbargeInPersistenceJobResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Ignite. */
    @IgniteInstanceResource
    private transient IgniteEx ignite;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Cache group names. */
    private Set<String> grpNames;

    /** Remove garbarge. */
    private final boolean removeGarbarge;

    /** Counter of processed partitions. */
    private final AtomicInteger processedPartitions = new AtomicInteger(0);

    /** Total partitions. */
    private volatile int totalPartitions;

    /** Last progress print timestamp. */
    private final AtomicLong lastProgressPrintTs = new AtomicLong(0);

    /** Calculation executor. */
    private volatile ExecutorService calcExecutor;

    /**
     * @param grpNames Cache group names.
     * @param removeGarbarge Clean up garbarge from partitions.
     */
    public VisorFindAndDeleteGarbargeInPersistenceClosure(Set<String> grpNames, boolean removeGarbarge) {
        this.grpNames = grpNames;
        this.removeGarbarge = removeGarbarge;
    }

    /** {@inheritDoc} */
    @Override public VisorFindAndDeleteGarbargeInPersistenceJobResult call() throws Exception {
        calcExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        try {
            return call0();
        }
        finally {
            calcExecutor.shutdown();
        }
    }

    /**
     *
     */
    private VisorFindAndDeleteGarbargeInPersistenceJobResult call0() {
        Set<Integer> grpIds = new HashSet<>();

        Set<String> missingCacheGroups = new HashSet<>();

        if (grpNames != null) {
            for (String grpName : grpNames) {
                CacheGroupContext groupContext = ignite.context().cache().cacheGroup(CU.cacheId(grpName));

                if (groupContext == null) {
                    missingCacheGroups.add(grpName);

                    continue;
                }

                grpIds.add(groupContext.groupId());
            }

            if (!missingCacheGroups.isEmpty()) {
                StringBuilder strBuilder = new StringBuilder("The following cache groups do not exist: ");

                for (String name : missingCacheGroups)
                    strBuilder.append(name).append(", ");

                strBuilder.delete(strBuilder.length() - 2, strBuilder.length());

                throw new IgniteException(strBuilder.toString());
            }
        }
        else {
            Collection<CacheGroupContext> groups = ignite.context().cache().cacheGroups();

            for (CacheGroupContext grp : groups) {
                if (!grp.systemCache() && !grp.isLocal())
                    grpIds.add(grp.groupId());
            }
        }

        List<Future<Map<Integer, Map<Integer, Long>>>> procPartFutures = new ArrayList<>();
        List<T2<CacheGroupContext, GridDhtLocalPartition>> partArgs = new ArrayList<>();

        for (Integer grpId : grpIds) {
            CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(grpId);

            List<GridDhtLocalPartition> parts = grpCtx.topology().localPartitions();

            for (GridDhtLocalPartition part : parts)
                partArgs.add(new T2<>(grpCtx, part));
        }

        // To decrease contention on same group.
        Collections.shuffle(partArgs);

        totalPartitions = partArgs.size();

        for (final T2<CacheGroupContext, GridDhtLocalPartition> t2 : partArgs)
            procPartFutures.add(calcExecutor.submit(new Callable<Map<Integer, Map<Integer, Long>>>() {
                @Override public Map<Integer, Map<Integer, Long>> call() throws Exception {
                    return processPartition(t2.get1(), t2.get2());
                }
            }));

        Map<Integer, Map<Integer, Long>> res = new HashMap<>();

        int curPart = 0;

        try {
            for (; curPart < procPartFutures.size(); curPart++) {
                Future<Map<Integer, Map<Integer, Long>>> fut = procPartFutures.get(curPart);

                Map<Integer, Map<Integer, Long>> partRes = fut.get();

                for (Map.Entry<Integer, Map<Integer, Long>> e : partRes.entrySet()) {
                    Map<Integer, Long> map = res.computeIfAbsent(e.getKey(), (x) -> new HashMap<>());

                    for (Map.Entry<Integer, Long> entry : e.getValue().entrySet()) {
                        map.compute(entry.getKey(), (k, v) -> (v == null ? 0 : v) + entry.getValue());
                    }
                }
            }

            if (removeGarbarge) {
                for (Map.Entry<Integer, Map<Integer, Long>> e : res.entrySet()) {
                    int grpId = e.getKey();

                    CacheGroupContext groupContext = ignite.context().cache().cacheGroup(grpId);

                    assert groupContext != null;

                    for (Integer cacheId : e.getValue().keySet()) {
                        groupContext.offheap().stopCache(cacheId, true);

                        ((GridCacheOffheapManager)
                            groupContext.offheap()).findAndCleanupLostIndexesForStoppedCache(cacheId);
                    }
                }
            }

            log.warning("VisorFindAndDeleteGarbargeInPersistenceClosure finished: processed " + totalPartitions + " partitions.");
        }
        catch (InterruptedException | ExecutionException | IgniteCheckedException e) {
            for (int j = curPart; j < procPartFutures.size(); j++)
                procPartFutures.get(j).cancel(false);

            throw unwrapFutureException(e);
        }

        return new VisorFindAndDeleteGarbargeInPersistenceJobResult(res);
    }

    /**
     * @param grpCtx Group context.
     * @param part Local partition.
     */
    private Map<Integer, Map<Integer, Long>> processPartition(
        CacheGroupContext grpCtx,
        GridDhtLocalPartition part
    ) {
        if (!part.reserve())
            return Collections.emptyMap();

        Map<Integer, Map<Integer, Long>> stoppedCachesForGrpId = new HashMap<>();

        try {
            if (part.state() != GridDhtPartitionState.OWNING)
                return Collections.emptyMap();

            GridIterator<CacheDataRow> it = grpCtx.offheap().partitionIterator(part.id());

            while (it.hasNextX()) {
                CacheDataRow row = it.nextX();

                if (row.cacheId() == 0) //TODO why we check this group?
                    break;

                int cacheId = row.cacheId();

                GridCacheContext cacheCtx = grpCtx.shared().cacheContext(row.cacheId());

                if (cacheCtx == null)
                    stoppedCachesForGrpId
                        .computeIfAbsent(grpCtx.groupId(), (x) -> new HashMap<>())
                        .compute(cacheId, (x, y) -> y == null? 1 : y + 1);
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to process partition [grpId=" + grpCtx.groupId() +
                ", partId=" + part.id() + "]", e);

            return Collections.emptyMap();
        }
        finally {
            part.release();
        }

        processedPartitions.incrementAndGet();

        printProgressIfNeeded();

        return stoppedCachesForGrpId;
    }

    /**
     *
     */
    private void printProgressIfNeeded() {
        long curTs = U.currentTimeMillis();
        long lastTs = lastProgressPrintTs.get();

        if (curTs - lastTs >= 60_000 && lastProgressPrintTs.compareAndSet(lastTs, curTs))
            log.warning("Current progress of VisorFindAndDeleteGarbargeInPersistenceClosure: checked "
                + processedPartitions.get() + " partitions out of " + totalPartitions);
    }

    /**
     * @param e Future result exception.
     * @return Unwrapped exception.
     */
    private IgniteException unwrapFutureException(Exception e) {
        assert e instanceof InterruptedException || e instanceof ExecutionException : "Expecting either InterruptedException " +
                "or ExecutionException";

        if (e instanceof InterruptedException)
            return new IgniteInterruptedException((InterruptedException)e);
        else if (e.getCause() instanceof IgniteException)
            return  (IgniteException)e.getCause();
        else
            return new IgniteException(e.getCause());
    }
}

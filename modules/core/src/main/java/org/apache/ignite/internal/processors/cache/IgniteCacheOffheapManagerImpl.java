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

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.database.RootPage;
import org.apache.ignite.internal.processors.cache.database.RowStore;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.local.GridLocalCache;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteComponentType.INDEXING;
import static org.apache.ignite.internal.pagemem.PageIdUtils.dwordsOffset;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/**
 *
 */
public class IgniteCacheOffheapManagerImpl extends GridCacheManagerAdapter implements IgniteCacheOffheapManager {
    /** */
    private boolean indexingEnabled;

    /** */
    private FreeList freeList;

    /** */
    private ReuseList reuseList;

    /** */
    private CacheDataStore locCacheDataStore;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        indexingEnabled = INDEXING.inClassPath() && GridQueryProcessor.isEnabled(cctx.config());

        if (cctx.affinityNode()) {
            IgniteCacheDatabaseSharedManager dbMgr = cctx.shared().database();

            int cpus = Runtime.getRuntime().availableProcessors();

            cctx.shared().database().checkpointReadLock();

            try {
                reuseList = new ReuseList(cctx.cacheId(), dbMgr.pageMemory(), cpus * 2, dbMgr.meta());
                freeList = new FreeList(cctx, reuseList);

                if (cctx.isLocal()) {
                    assert cctx.cache() instanceof GridLocalCache : cctx.cache();

                    locCacheDataStore = createCacheDataStore(0, (GridLocalCache)cctx.cache());
                }
            }
            finally {
                cctx.shared().database().checkpointReadUnlock();
            }
        }
    }

    /**
     * @param part Partition.
     * @return Data store for given entry.
     */
    private CacheDataStore dataStore(GridDhtLocalPartition part) {
        if (cctx.isLocal())
            return locCacheDataStore;
        else {
            assert part != null;

            return part.dataStore();
        }
    }

    /**
     * @param p Partition.
     * @return Partition data.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private CacheDataStore partitionData(int p) throws IgniteCheckedException {
        if (cctx.isLocal())
            return locCacheDataStore;
        else {
            GridDhtLocalPartition part = cctx.topology().localPartition(p, AffinityTopologyVersion.NONE, false);

            return part != null ? part.dataStore() : null;
        }
    }

    /** {@inheritDoc} */
    @Override public ReuseList reuseList() {
        return reuseList;
    }

    /** {@inheritDoc} */
    @Override public FreeList freeList() {
        return freeList;
    }

    /** {@inheritDoc} */
    @Override public long entriesCount(boolean primary, boolean backup, AffinityTopologyVersion topVer) throws IgniteCheckedException {
        if (cctx.isLocal())
            return 0; // TODO: GG-11208.
        else {
            ClusterNode locNode = cctx.localNode();

            long cnt = 0;

            for (GridDhtLocalPartition locPart : cctx.topology().currentLocalPartitions()) {
                if (primary) {
                    if (cctx.affinity().primary(locNode, locPart.id(), topVer)) {
                        cnt += locPart.size();

                        continue;
                    }
                }

                if (backup) {
                    if (cctx.affinity().backup(locNode, locPart.id(), topVer))
                        cnt += locPart.size();
                }
            }

            return cnt;
        }
    }

    /** {@inheritDoc} */
    @Override public long entriesCount(int part) {
        if (cctx.isLocal())
            return 0; // TODO: GG-11208.
        else {
            GridDhtLocalPartition locPart = cctx.topology().localPartition(part, AffinityTopologyVersion.NONE, false);

            return locPart == null ? 0 : locPart.size();
        }
    }

    /**
     * @param primary Primary data flag.
     * @param backup Primary data flag.
     * @param topVer Topology version.
     * @return Data stores iterator.
     */
    private Iterator<CacheDataStore> cacheData(boolean primary, boolean backup, AffinityTopologyVersion topVer) {
        assert primary || backup;

        if (cctx.isLocal())
            return Collections.singleton(locCacheDataStore).iterator();
        else {
            final Iterator<GridDhtLocalPartition> it = cctx.topology().currentLocalPartitions().iterator();

            if (primary && backup) {
                return F.iterator(it, new IgniteClosure<GridDhtLocalPartition, CacheDataStore>() {
                    @Override public CacheDataStore apply(GridDhtLocalPartition part) {
                        return part.dataStore();
                    }
                }, true);
            }

            final Set<Integer> parts = primary ? cctx.affinity().primaryPartitions(cctx.localNodeId(), topVer) :
                    cctx.affinity().backupPartitions(cctx.localNodeId(), topVer);

            return F.iterator(it, new IgniteClosure<GridDhtLocalPartition, CacheDataStore>() {
                        @Override public CacheDataStore apply(GridDhtLocalPartition part) {
                            return part.dataStore();
                        }
                    }, true,
                    new IgnitePredicate<GridDhtLocalPartition>() {
                        @Override public boolean apply(GridDhtLocalPartition part) {
                            return parts.contains(part.id());
                        }
                    });
        }
    }

    /** {@inheritDoc} */
    @Override public UpdateInfo update(
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            int partId,
            GridDhtLocalPartition part
    ) throws IgniteCheckedException {
        UpdateInfo updInfo = dataStore(part).update(key, partId, val, ver, expireTime);

        if (indexingEnabled) {
            GridCacheQueryManager qryMgr = cctx.queries();

            assert qryMgr.enabled();

            qryMgr.store(key, partId, val, ver, expireTime);
        }
        return updInfo;
    }

    /** {@inheritDoc} */
    @Override public CacheObjectEntry remove(
            KeyCacheObject key,
            CacheObject prevVal,
            GridCacheVersion prevVer,
            int partId,
            GridDhtLocalPartition part
    ) throws IgniteCheckedException {
        CacheObjectEntry removed = dataStore(part).remove(key);

        if (indexingEnabled) {
            GridCacheQueryManager qryMgr = cctx.queries();

            assert qryMgr.enabled();

            qryMgr.remove(key, partId, prevVal, prevVer);
        }
        return removed;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable public CacheObjectEntry read(GridCacheMapEntry entry)
        throws IgniteCheckedException {
        try {
            KeyCacheObject key = entry.key();

            assert cctx.isLocal() || entry.localPartition() != null : entry;

            return dataStore(entry.localPartition()).find(key);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to read entry: " + entry.key(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(GridCacheMapEntry entry) {
        try {
            return read(entry) != null;
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to read value", e);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public void onPartitionCounterUpdated(int part, long cntr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public long lastUpdatedPartitionCounter(int part) {
        return 0;
    }

    /**
     * Clears offheap entries.
     *
     * @param readers {@code True} to clear readers.
     */
    @SuppressWarnings("unchecked")
    @Override public void clear(boolean readers) {
        GridCacheVersion obsoleteVer = null;

        GridIterator<CacheDataRow>  it = rowsIterator(true, true, null);

        while (it.hasNext()) {
            KeyCacheObject key = it.next().key();

            try {
                if (obsoleteVer == null)
                    obsoleteVer = cctx.versions().next();

                GridCacheEntryEx entry = cctx.cache().entryEx(key);

                entry.clear(obsoleteVer, readers);
            }
            catch (GridDhtInvalidPartitionException ignore) {
                // Ignore.
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to clear cache entry: " + key, e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public int onUndeploy(ClassLoader ldr) {
        // TODO: GG-11141.
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long offHeapAllocatedSize() {
        // TODO GG-10884.
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Iterable<GridCacheBatchSwapEntry> swapped) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @param primary {@code True} if need return primary entries.
     * @param backup {@code True} if need return backup entries.
     * @param topVer Topology version to use.
     * @return Entries iterator.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    @Override  public <K, V> GridCloseableIterator<Cache.Entry<K, V>> entriesIterator(final boolean primary,
        final boolean backup,
        final AffinityTopologyVersion topVer,
        final boolean keepBinary) throws IgniteCheckedException {
        final Iterator<CacheDataRow> it = rowsIterator(primary, backup, topVer);

        return new GridCloseableIteratorAdapter<Cache.Entry<K, V>>() {
            /** */
            private CacheEntryImplEx next;

            @Override protected Cache.Entry<K, V> onNext() throws IgniteCheckedException {
                CacheEntryImplEx ret = next;

                next = null;

                return ret;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (next != null)
                    return true;

                CacheDataRow nextRow = null;

                if (it.hasNext())
                    nextRow = it.next();

                if (nextRow != null) {
                    KeyCacheObject key = nextRow.key();
                    CacheObject val = nextRow.value();

                    Object key0 = cctx.unwrapBinaryIfNeeded(key, keepBinary, false);
                    Object val0 = cctx.unwrapBinaryIfNeeded(val, keepBinary, false);

                    next = new CacheEntryImplEx(key0, val0, nextRow.version());

                    return true;
                }

                return false;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public GridCloseableIterator<KeyCacheObject> keysIterator(final int part) throws IgniteCheckedException {
        CacheDataStore data = partitionData(part);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        final GridCursor<? extends CacheDataRow> cur = data.cursor();

        return new GridCloseableIteratorAdapter<KeyCacheObject>() {
            /** */
            private KeyCacheObject next;

            @Override protected KeyCacheObject onNext() throws IgniteCheckedException {
                KeyCacheObject res = next;

                next = null;

                return res;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (next != null)
                    return true;

                if (cur.next()) {
                    CacheDataRow row = cur.get();

                    next = row.key();
                }

                return next != null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> iterator(boolean primary, boolean backups, final AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        return rowsIterator(primary, backups, topVer);
    }

    /**
     * @param primary Primary entries flag.
     * @param backups Backup entries flag.
     * @param topVer Topology version.
     * @return Iterator.
     */
    private GridIterator<CacheDataRow> rowsIterator(boolean primary, boolean backups, AffinityTopologyVersion topVer) {
        final Iterator<CacheDataStore> dataIt = cacheData(primary, backups, topVer);

        return new GridCloseableIteratorAdapter<CacheDataRow>() {
            /** */
            private GridCursor<? extends CacheDataRow> cur;

            /** */
            private CacheDataRow next;

            @Override protected CacheDataRow onNext() throws IgniteCheckedException {
                CacheDataRow res = next;

                next = null;

                return res;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (next != null)
                    return true;

                while (true) {
                    if (cur == null) {
                        if (dataIt.hasNext())
                            cur = dataIt.next().cursor();
                        else
                            break;
                    }

                    if (cur.next()) {
                        next = cur.get();

                        break;
                    }
                    else
                        cur = null;
                }

                return next != null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public GridIterator<CacheDataRow> iterator(int part) throws IgniteCheckedException {
        CacheDataStore data = partitionData(part);

        if (data == null)
            return new GridEmptyCloseableIterator<>();

        final GridCursor<? extends CacheDataRow> cur = data.cursor();

        return new GridCloseableIteratorAdapter<CacheDataRow>() {
            /** */
            private CacheDataRow next;

            @Override protected CacheDataRow onNext() throws IgniteCheckedException {
                CacheDataRow res = next;

                next = null;

                return res;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (next != null)
                    return true;

                if (cur.next())
                    next = cur.get();

                return next != null;
            }
        };
    }

    /**
     * @param pageId Page ID.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    private Page page(long pageId) throws IgniteCheckedException {
        return cctx.shared().database().pageMemory().page(cctx.cacheId(), pageId);
    }

    /** {@inheritDoc} */
    @Override public final CacheDataStore createCacheDataStore(int p, CacheDataStore.Listener lsnr) throws IgniteCheckedException {
        IgniteCacheDatabaseSharedManager dbMgr = cctx.shared().database();

        String idxName = treeName(p);

        // TODO: GG-11220 cleanup when cache/partition is destroyed.
        final RootPage rootPage = dbMgr.meta().getOrAllocateForTree(cctx.cacheId(), idxName);

        CacheDataRowStore rowStore = new CacheDataRowStore(cctx, freeList);

        CacheDataTree dataTree = new CacheDataTree(idxName,
                reuseList,
                rowStore,
                cctx,
                dbMgr.pageMemory(),
                rootPage.pageId(),
                rootPage.isAllocated());

        return new CacheDataStoreImpl(rowStore, dataTree, lsnr);
    }

    /**
     * @param p Partition.
     * @return Tree name for given partition.
     */
    private String treeName(int p) {
        return BPlusTree.treeName("p-" + p, cctx.cacheId(), "CacheData");
    }

    /** {@inheritDoc} */
    @Override public PendingEntries createPendingEntries() throws IgniteCheckedException {
        IgniteCacheDatabaseSharedManager dbMgr = cctx.shared().database();
        String btname = BPlusTree.treeName("pending", cctx.cacheId(), "PendingEntries");
        final RootPage rootPage = dbMgr.meta().getOrAllocateForTree(cctx.cacheId(), btname);

        return new PendingEntriesImpl(
            btname,
            cctx.cacheId(),
            dbMgr.pageMemory(),
            rootPage.pageId(),
            reuseList,
            rootPage.isAllocated());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteCacheOffheapManagerImpl.class, this);
    }


    /**
     *
     */
    private class CacheDataStoreImpl implements CacheDataStore {
        /** */
        private final CacheDataRowStore rowStore;

        /** */
        private final CacheDataTree dataTree;

        /** */
        private final Listener lsnr;

        /**
         * @param rowStore Row store.
         * @param dataTree Data tree.
         * @param lsnr Listener.
         */
        public CacheDataStoreImpl(CacheDataRowStore rowStore,
            CacheDataTree dataTree,
            Listener lsnr) {
            this.rowStore = rowStore;
            this.dataTree = dataTree;
            this.lsnr = lsnr;
        }

        /** {@inheritDoc} */
        @Override public UpdateInfo update(KeyCacheObject key,
            int p,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime) throws IgniteCheckedException {
            DataRow dataRow = new DataRow(key.hashCode(), key, val, ver, p, expireTime);

            rowStore.addRow(dataRow);

            DataRow old = dataTree.put(dataRow);
            CacheObjectEntry oldEntry = null;

            if (old == null)
                lsnr.onInsert();
            else {
                assert old.link != 0 : old;
                oldEntry = createEntry(key, old);
                rowStore.removeRow(old.link);
            }

            DataRow dr = new DataRow(0, dataRow.link);
            dr.initData();

            return new UpdateInfo(dataRow.link(), oldEntry);
        }

        /** {@inheritDoc} */
        @Override public CacheObjectEntry remove(KeyCacheObject key) throws IgniteCheckedException {
            DataRow dataRow = dataTree.remove(new KeySearchRow(key.hashCode(), key, 0));
            CacheObjectEntry removed = null;

            if (dataRow != null) {
                assert dataRow.link != 0 : dataRow;

                removed = new CacheObjectEntry(key, null, dataRow.version(), dataRow.expireTime(),
                    dataRow.link());

                rowStore.removeRow(dataRow.link);

                lsnr.onRemove();
            }
            return removed;
        }

        /** {@inheritDoc} */
        @Override public CacheObjectEntry find(KeyCacheObject key)
            throws IgniteCheckedException {
            DataRow dataRow = dataTree.findOne(new KeySearchRow(key.hashCode(), key, 0));

            return dataRow != null ?
                new CacheObjectEntry(key, dataRow.value(), dataRow.version(), dataRow.expireTime(), dataRow.link())
                : null;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
            return dataTree.find(null, null);
        }

        private CacheObjectEntry createEntry(KeyCacheObject key, DataRow dataRow) {
            return dataRow != null && dataRow.link() != 0 ?
                new CacheObjectEntry(key, dataRow.value(), dataRow.version(), dataRow.expireTime(), dataRow.link())
                : null;
        }
    }

    /**
     *
     */
    private class KeySearchRow {
        /** */
        int hash;

        /** */
        KeyCacheObject key;

        /** */
        long link;

        /**
         * @param hash Hash code.
         * @param key Key.
         * @param link Link.
         */
        KeySearchRow(int hash, KeyCacheObject key, long link) {
            this.hash = hash;
            this.key = key;
            this.link = link;
        }

        /**
         * @param buf Buffer.
         * @throws IgniteCheckedException If failed.
         */
        protected void doInitData(ByteBuffer buf) throws IgniteCheckedException {
            key = cctx.cacheObjects().toKeyCacheObject(cctx.cacheObjectContext(), buf);
        }

        /**
         * Init data.
         */
        protected final void initData() {
            if (key != null)
                return;

            assert link != 0;

            try (Page page = page(pageId(link))) {
                ByteBuffer buf = page.getForRead();

                try {
                    DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

                    int dataOff = io.getDataOffset(buf, dwordsOffset(link));

                    buf.position(dataOff);

                    // Skip entry size.
                    buf.getShort();

                    doInitData(buf);
                }
                finally {
                    page.releaseRead();
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * @return Key.
         */
        public KeyCacheObject key() {
            initData();

            return key;
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(KeySearchRow.class, this);
        }
    }

    /**
     *
     */
    class DataRow extends KeySearchRow implements CacheDataRow {
        /** */
        private CacheObject val;

        /** */
        private GridCacheVersion ver;

        /** */
        private int part = -1;

        /** */
        private long expTime;

        /**
         * @param hash Hash code.
         * @param link Link.
         */
        DataRow(int hash, long link) {
            super(hash, null, link);

            part = PageIdUtils.partId(link);
        }

        /**
         * @param hash Hash code.
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @param part Partition.
         */
        DataRow(int hash, KeyCacheObject key, CacheObject val, GridCacheVersion ver, int part) {
            this(hash, key, val, ver, part, 0);
        }

        /**
         * @param hash Hash code.
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @param part Partition.
         */
        DataRow(int hash, KeyCacheObject key, CacheObject val, GridCacheVersion ver, int part, long expTime) {
            super(hash, key, 0);

            this.val = val;
            this.ver = ver;
            this.part = part;
            this.expTime = expTime;
        }

        /** {@inheritDoc} */
        @Override protected void doInitData(ByteBuffer buf) throws IgniteCheckedException {
            key = cctx.cacheObjects().toKeyCacheObject(cctx.cacheObjectContext(), buf);
            val = cctx.cacheObjects().toCacheObject(cctx.cacheObjectContext(), buf);

            int topVer = buf.getInt();
            int nodeOrderDrId = buf.getInt();
            long globalTime = buf.getLong();
            long order = buf.getLong();

            ver = new GridCacheVersion(topVer, nodeOrderDrId, globalTime, order);
            expTime = buf.getLong();
        }

        /** {@inheritDoc} */
        @Override public CacheObject value() {
            initData();

            return val;
        }

        /** {@inheritDoc} */
        @Override public GridCacheVersion version() {
            initData();

            return ver;
        }

        /** {@inheritDoc} */
        @Override public int partition() {
            assert part != -1;

            return part;
        }

        /** {@inheritDoc} */
        @Override public long link() {
            return link;
        }

        /** {@inheritDoc} */
        @Override public void link(long link) {
            this.link = link;
        }

        /** {@inheritDoc} */
        @Override public long expireTime() {
            initData();
            return expTime;
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(DataRow.class, this);
        }
    }

    /**
     *
     */
    private static class CacheDataTree extends BPlusTree<KeySearchRow, DataRow> {
        /** */
        private final CacheDataRowStore rowStore;

        /** */
        private final GridCacheContext cctx;

        /**
         * @param name Tree name.
         * @param reuseList Reuse list.
         * @param rowStore Row store.
         * @param cctx Context.
         * @param pageMem Page memory.
         * @param metaPageId Meta page ID.
         * @param initNew Initialize new index.
         * @throws IgniteCheckedException If failed.
         */
        CacheDataTree(
            String name,
            ReuseList reuseList,
            CacheDataRowStore rowStore,
            GridCacheContext cctx,
            PageMemory pageMem,
            FullPageId metaPageId,
            boolean initNew)
            throws IgniteCheckedException
        {
            super(name, cctx.cacheId(), pageMem, metaPageId, reuseList, DataInnerIO.VERSIONS, DataLeafIO.VERSIONS);

            assert rowStore != null;

            this.rowStore = rowStore;
            this.cctx = cctx;

            if (initNew)
                initNew();
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<KeySearchRow> io, ByteBuffer buf, int idx, KeySearchRow row)
            throws IgniteCheckedException {
            KeySearchRow row0 = io.getLookupRow(this, buf, idx);

            int cmp = Integer.compare(row0.hash, row.hash);

            if (cmp != 0)
                return cmp;

            return compareKeys(row0.key(), row.key());
        }

        /** {@inheritDoc} */
        @Override protected DataRow getRow(BPlusIO<KeySearchRow> io, ByteBuffer buf, int idx)
            throws IgniteCheckedException {
            int hash = ((RowLinkIO)io).getHash(buf, idx);
            long link = ((RowLinkIO)io).getLink(buf, idx);

            return rowStore.dataRow(hash, link);
        }

        /**
         * @param key1 First key.
         * @param key2 Second key.
         * @return Compare result.
         * @throws IgniteCheckedException If failed.
         */
        private int compareKeys(CacheObject key1, CacheObject key2) throws IgniteCheckedException {
            byte[] bytes1 = key1.valueBytes(cctx.cacheObjectContext());
            byte[] bytes2 = key2.valueBytes(cctx.cacheObjectContext());

            int len = Math.min(bytes1.length, bytes2.length);

            for (int i = 0; i < len; i++) {
                byte b1 = bytes1[i];
                byte b2 = bytes2[i];

                if (b1 != b2)
                    return b1 > b2 ? 1 : -1;
            }

            return Integer.compare(bytes1.length, bytes2.length);
        }
    }

    /**
     *
     */
    private class CacheDataRowStore extends RowStore {
        /**
         * @param cctx Cache context.
         * @param freeList Free list.
         */
        CacheDataRowStore(GridCacheContext<?, ?> cctx, FreeList freeList) {
            super(cctx, freeList);
        }

        /**
         * @param hash Hash code.
         * @param link Link.
         * @return Search row.
         * @throws IgniteCheckedException If failed.
         */
        private KeySearchRow keySearchRow(int hash, long link) throws IgniteCheckedException {
            return new KeySearchRow(hash, null, link);
        }

        /**
         * @param hash Hash code.
         * @param link Link.
         * @return Data row.
         * @throws IgniteCheckedException If failed.
         */
        private DataRow dataRow(int hash, long link) throws IgniteCheckedException {
            return new DataRow(hash, link);
        }
    }

    /**
     *
     */
    private interface RowLinkIO {
        /**
         * @param buf Buffer.
         * @param idx Index.
         * @return Row link.
         */
        public long getLink(ByteBuffer buf, int idx);

        /**
         * @param buf Buffer.
         * @param idx Index.
         * @return Key hash code.
         */
        public int getHash(ByteBuffer buf, int idx);
    }

    /**
     *
     */
    private static class DataInnerIO extends BPlusInnerIO<KeySearchRow> implements RowLinkIO {
        /** */
        public static final IOVersions<DataInnerIO> VERSIONS = new IOVersions<>(
            new DataInnerIO(1)
        );

        /**
         * @param ver Page format version.
         */
        DataInnerIO(int ver) {
            super(T_DATA_REF_INNER, ver, true, 12);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer buf, int idx, KeySearchRow row) {
            assert row.link != 0;

            setHash(buf, idx, row.hash);
            setLink(buf, idx, row.link);
        }

        /** {@inheritDoc} */
        @Override public KeySearchRow getLookupRow(BPlusTree<KeySearchRow,?> tree, ByteBuffer buf, int idx)
            throws IgniteCheckedException {
            int hash = getHash(buf, idx);
            long link = getLink(buf, idx);

            return ((CacheDataTree)tree).rowStore.keySearchRow(hash, link);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer dst, int dstIdx, BPlusIO<KeySearchRow> srcIo, ByteBuffer src, int srcIdx) {
            int hash = ((RowLinkIO)srcIo).getHash(src, srcIdx);
            long link = ((RowLinkIO)srcIo).getLink(src, srcIdx);

            setHash(dst, dstIdx, hash);
            setLink(dst, dstIdx, link);
        }

        /** {@inheritDoc} */
        @Override public long getLink(ByteBuffer buf, int idx) {
            assert idx < getCount(buf): idx;

            return buf.getLong(offset(idx, SHIFT_LINK));
        }

        /**
         * @param buf Buffer.
         * @param idx Index.
         * @param link Link.
         */
        private void setLink(ByteBuffer buf, int idx, long link) {
            buf.putLong(offset(idx, SHIFT_LINK), link);

            assert getLink(buf, idx) == link;
        }


        /** {@inheritDoc} */
        @Override public int getHash(ByteBuffer buf, int idx) {
            return buf.getInt(offset(idx) + 8);
        }

        /**
         * @param buf Buffer.
         * @param idx Index.
         * @param hash Hash.
         */
        private void setHash(ByteBuffer buf, int idx, int hash) {
            buf.putInt(offset(idx) + 8, hash);
        }
    }

    /**
     *
     */
    private static class DataLeafIO extends BPlusLeafIO<KeySearchRow> implements RowLinkIO {
        /** */
        public static final IOVersions<DataLeafIO> VERSIONS = new IOVersions<>(
            new DataLeafIO(1)
        );

        /**
         * @param ver Page format version.
         */
        DataLeafIO(int ver) {
            super(T_DATA_REF_LEAF, ver, 12);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer buf, int idx, KeySearchRow row) {
            DataRow row0 = (DataRow)row;

            assert row0.link != 0;

            setHash(buf, idx, row0.hash);
            setLink(buf, idx, row0.link);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer dst, int dstIdx, BPlusIO<KeySearchRow> srcIo, ByteBuffer src, int srcIdx)
            throws IgniteCheckedException {
            setHash(dst, dstIdx, getHash(src, srcIdx));
            setLink(dst, dstIdx, getLink(src, srcIdx));
        }

        /** {@inheritDoc} */
        @Override public KeySearchRow getLookupRow(BPlusTree<KeySearchRow,?> tree, ByteBuffer buf, int idx)
            throws IgniteCheckedException {

            int hash = getHash(buf, idx);
            long link = getLink(buf, idx);

            return ((CacheDataTree)tree).rowStore.keySearchRow(hash, link);
        }

        /** {@inheritDoc} */
        @Override public long getLink(ByteBuffer buf, int idx) {
            assert idx < getCount(buf): idx;

            return buf.getLong(offset(idx));
        }

        /**
         * @param buf Buffer.
         * @param idx Index.
         * @param link Link.
         */
        private void setLink(ByteBuffer buf, int idx, long link) {
            buf.putLong(offset(idx), link);

            assert getLink(buf, idx) == link;
        }

        /** {@inheritDoc} */
        @Override public int getHash(ByteBuffer buf, int idx) {
            return buf.getInt(offset(idx) + 8);
        }

        /**
         * @param buf Buffer.
         * @param idx Index.
         * @param hash Hash.
         */
        private void setHash(ByteBuffer buf, int idx, int hash) {
            buf.putInt(offset(idx) + 8, hash);
        }
    }


    /**
     * The class to store tracked entry on the paged memory.
     * We have to store only link and expireTime because entry could be loaded from swap
     * by link.
     */
    class PendingRow {
        private static final int SIZE = 8 + 8;

        /** Expire time. */
        long expireTime;

        /** Link. */
        long link;

        /**
         * @param entry Entry.
         */
        PendingRow(GridCacheMapEntry entry) {
            link = entry.link();
            expireTime = entry.expireTimeExtras();

            assert link != 0;
            assert expireTime > 0;
        }

        /**
         * @param entry Entry.
         */
        PendingRow(CacheObjectEntry entry) {
            link = entry.link();
            expireTime = entry.expireTime();

            assert link != 0;
            assert expireTime > 0;
        }

        /**
         * @param time Time.
         */
        PendingRow(long time) {
            expireTime = time;
        }

        /**
         * @param expireTime Expire time.
         * @param link Link.
         */
        PendingRow(long expireTime, long link) {
            this.expireTime = expireTime;
            this.link= link;
        }

        /**
         *
         */
        GridCacheEntryEx entryEx() {
            DataRow dr = new DataRow(0, link);
            dr.initData();
            return cctx.cache().entryEx(dr.key());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "PendingRow{" +
                "expireTime=" + expireTime +
                ", link=" + link +
                '}';
        }
    }

    /**
     * Implementation of tracked entries collection based on B+tree.
     */
    class PendingEntriesImpl extends BPlusTree<PendingRow, PendingRow> implements PendingEntries {

        /**
         * @param name Tree name.
         * @param cacheId Cache ID.
         * @param pageMem Page memory.
         * @param metaPageId Meta page ID.
         * @param reuseList Reuse list.
         */
        public PendingEntriesImpl(String name, int cacheId, PageMemory pageMem, FullPageId metaPageId,
            ReuseList reuseList, boolean initNew) throws IgniteCheckedException {
            super(name, cacheId, pageMem, metaPageId, reuseList,
                PendingEntryInnerIO.VERSIONS, PendingEntryLeafIO.VERSIONS);

            if(initNew)
                initNew();
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<PendingRow> io, ByteBuffer buf, int idx,
            PendingRow row) throws IgniteCheckedException {
            PendingRow row0 = io.getLookupRow(this, buf, idx);
            int cmp = Long.compare(row0.expireTime, row.expireTime);
            return (cmp != 0) ? cmp : Long.compare(row0.link, row.link);
        }

        /** {@inheritDoc} */
        @Override
        protected PendingRow getRow(BPlusIO<PendingRow> io,
            ByteBuffer buf, int idx) throws IgniteCheckedException {
            return io.getLookupRow(this, buf, idx);
        }

        /** {@inheritDoc} */
        @Override public void addTrackedEntry(GridCacheMapEntry entry) {
            try {
                PendingRow r = new PendingRow(entry);
                put(r);
            }
            catch (IgniteCheckedException e) {
                log.error("Unexpected exception", e);
            }
        }

        /** {@inheritDoc} */
        @Override public void removeTrackedEntry(GridCacheMapEntry entry) {
            removeTrackedEntry(new PendingRow(entry));
        }

        /** {@inheritDoc} */
        @Override public void removeTrackedEntry(CacheObjectEntry entry) {
            removeTrackedEntry(new PendingRow(entry));
        }

        /** {@inheritDoc} */
        private void removeTrackedEntry(PendingRow r) {
            try {
                remove(r);
            }
            catch (IgniteCheckedException e) {
                log.error("Unexpected exception", e);
            }
        }

        /** {@inheritDoc} */
        @Override public ExpiredEntriesCursor expired(final long time) {
            final GridCursor<PendingRow> cur;
            try {
                cur = find(new PendingRow(0), new PendingRow(time));
                return new ExpiredEntriesCursor() {
                    List<PendingRow> rows = new ArrayList<>();

                    @Override public void removeAll() {
                        try {
                            while (cur.next()) {
                                PendingRow r = cur.get();
                                if(r.expireTime < time)
                                    rows.add(r);
                            }

                            for (PendingRow r : rows)
                                remove(r);
                        }
                        catch (IgniteCheckedException e) {
                            log.error("Unexpected exception", e);
                        }
                    }

                    @Override public boolean next() throws IgniteCheckedException {
                        if (!cur.next())
                            return false;
                        return cur.get().expireTime < time;
                    }

                    @Override public GridCacheEntryEx get() throws IgniteCheckedException {
                        PendingRow r = cur.get();
                        if ((r == null) || (r.expireTime >= time))
                            return null;
                        rows.add(r);
                        return r.entryEx();
                    }
                };
            }
            catch (IgniteCheckedException e) {
                log.error("Unexpected exception", e);
                return new ExpiredEntriesCursor() {
                    @Override public void removeAll() {
                        // No-op.
                    }

                    @Override public boolean next() throws IgniteCheckedException {
                        return false;
                    }

                    @Override public GridCacheEntryEx get() throws IgniteCheckedException {
                        return null;
                    }
                };
            }
        }

        /**
         * @param expireTime Expire time.
         * @param link Link.
         */
        PendingRow createPendingRow(long expireTime, long link) {
            return new PendingRow(expireTime, link);
        }

        /** {@inheritDoc} */
        @Override public int pendingSize() {
            try {
                return (int)size();
            }
            catch (IgniteCheckedException e) {
                log.error("Unexpected exception", e);
                return -1;
            }
        }

        /** {@inheritDoc} */
        @Override public long firstExpired() {
            try {
                GridCursor<PendingRow> cur = find(null, null);
                if (cur.next())
                    return cur.get().expireTime;
            }
            catch (IgniteCheckedException e) {
                log.error("Unexpected exception", e);
            }
            return 0;
        }
    }

    /**
     *
     */
    private interface PendingRowIO {
        /**
         * @param buf Buffer.
         * @param idx Index.
         */
        long getExpireTime(ByteBuffer buf, int idx);
        /**
         * @param buf Buffer.
         * @param idx Index.
         */
        long getLink(ByteBuffer buf, int idx);
    }

    /**
     *
     */
    private static class PendingEntryInnerIO extends BPlusInnerIO<PendingRow> implements PendingRowIO {
        /** */
        public static final IOVersions<PendingEntryInnerIO> VERSIONS = new IOVersions<>(
            new PendingEntryInnerIO(1)
        );

        /**
         * @param ver Page format version.
         */
        PendingEntryInnerIO(int ver) {
            super(T_PENDING_REF_INNER, ver, true, PendingRow.SIZE);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer buf, int idx,
            PendingRow row) throws IgniteCheckedException {
            setExpireTime(row.expireTime, buf, idx);
            setLink(row.link, buf, idx);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer dst, int dstIdx, BPlusIO<PendingRow> srcIo,
            ByteBuffer src, int srcIdx) throws IgniteCheckedException {
            setExpireTime(((PendingRowIO)srcIo).getExpireTime(src, srcIdx), dst, dstIdx);
            setLink(((PendingRowIO)srcIo).getLink(src, srcIdx), dst, dstIdx);
        }

        /** {@inheritDoc} */
        @Override public PendingRow getLookupRow(
            BPlusTree<PendingRow, ?> tree, ByteBuffer buf,
            int idx) throws IgniteCheckedException {
            return ((PendingEntriesImpl)tree).createPendingRow(
                getExpireTime(buf, idx),
                getLink(buf, idx));
        }

        /** {@inheritDoc} */
        @Override public long getExpireTime(ByteBuffer buf, int idx) {
            return buf.getLong(offset(idx));
        }

        /**
         * @param expireTime Expire time.
         * @param buf Buffer.
         * @param idx Index.
         */
        private void setExpireTime(long expireTime, ByteBuffer buf, int idx) {
            buf.putLong(offset(idx), expireTime);
        }

        /** {@inheritDoc} */
        @Override public long getLink(ByteBuffer buf, int idx) {
            return buf.getLong(offset(idx) + 8);
        }

        /**
         * @param link Link.
         * @param buf Buffer.
         * @param idx Index.
         */
        private void setLink(long link, ByteBuffer buf, int idx) {
            buf.putLong(offset(idx) + 8, link);
        }
    }

    /**
     *
     */
    private static class PendingEntryLeafIO extends BPlusLeafIO<PendingRow> implements PendingRowIO {
        /** */
        public static final IOVersions<PendingEntryLeafIO> VERSIONS = new IOVersions<>(
            new PendingEntryLeafIO(1)
        );

        /**
         * @param ver Page format version.
         */
        PendingEntryLeafIO(int ver) {
            super(T_PENDING_REF_LEAF, ver, PendingRow.SIZE);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer buf, int idx,
            PendingRow row) throws IgniteCheckedException {
            setExpireTime(row.expireTime, buf, idx);
            setLink(row.link, buf, idx);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer dst, int dstIdx, BPlusIO<PendingRow> srcIo,
            ByteBuffer src, int srcIdx) throws IgniteCheckedException {
            setExpireTime(((PendingRowIO)srcIo).getExpireTime(src, srcIdx), dst, dstIdx);
            setLink(((PendingRowIO)srcIo).getLink(src, srcIdx), dst, dstIdx);
        }

        /** {@inheritDoc} */
        @Override public PendingRow getLookupRow(
            BPlusTree<PendingRow, ?> tree, ByteBuffer buf,
            int idx) throws IgniteCheckedException {
            return ((PendingEntriesImpl)tree).createPendingRow(
                getExpireTime(buf, idx),
                getLink(buf, idx));
        }

        /** {@inheritDoc} */
        @Override public long getExpireTime(ByteBuffer buf, int idx) {
            return buf.getLong(offset(idx));
        }

        /**
         * @param expireTime Expire time.
         * @param buf Buffer.
         * @param idx Index.
         */
        private void setExpireTime(long expireTime, ByteBuffer buf, int idx) {
            buf.putLong(offset(idx), expireTime);
        }

        /** {@inheritDoc} */
        @Override public long getLink(ByteBuffer buf, int idx) {
            return buf.getLong(offset(idx) + 8);
        }

        /**
         * @param link Link.
         * @param buf Buffer.
         * @param idx Index.
         */
        private void setLink(long link, ByteBuffer buf, int idx) {
            buf.putLong(offset(idx) + 8, link);
        }
    }
}

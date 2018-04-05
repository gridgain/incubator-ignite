package org.apache.ignite.internal.pagemem;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;

public class DataStructureSizeManager {
    /** */
    public static final String TOTAL = "total";

    /** */
    public static final String INDEX = "indexes";

    /** */
    public static final String INDEX_TREE = "indexes-tree";

    /** */
    public static final String INDEX_REUSE_LIST = "indexes-reuse-list";

    /** */
    public static final String PARTITION = "partition";

    /** */
    public static final String PK_INDEX = "pk-index";

    /** */
    public static final String REUSE_LIST = "reuse-list";

    /** */
    public static final String DATA = "data";

    /** */
    public static final String PURE_DATA = "pure-data";

    /** */
    public static final String INTERNAL = "internal";

    /** */
    private final Map<String, DataStructureSize> sizes = new LinkedHashMap<>();

    /** */
    private final String grpName;

    private final int pageSize;

    /**
     * @param grpCtx Cache group context.
     */
    public DataStructureSizeManager(CacheGroupContext grpCtx) {
        grpName = grpCtx.cacheOrGroupName();

        pageSize = grpCtx.dataRegion().pageMemory().pageSize();

        String indexesTree = grpName + "-" + INDEX_TREE;
        String indexesReuseList = grpName + "-" + INDEX_REUSE_LIST;

        DataStructureSize indexesTreePages = simpleTracker(indexesTree);
        DataStructureSize indexesReuseListPages = simpleTracker(indexesReuseList);

        sizes.put(indexesTree, indexesTreePages);
        sizes.put(indexesReuseList, indexesReuseListPages);

        String pkIndex = grpName + "-" + PK_INDEX;
        String reuseList = grpName + "-" + REUSE_LIST;
        String data = grpName + "-" + DATA;
        String pureData = grpName + "-" + PURE_DATA;

        DataStructureSize pkIndexPages = simpleTracker(pkIndex);
        DataStructureSize reuseListPages = simpleTracker(reuseList);
        DataStructureSize dataPages = simpleTracker(data);
        DataStructureSize pureDataSize = simpleTracker(pureData);

        sizes.put(pkIndex, pkIndexPages);
        sizes.put(reuseList, reuseListPages);
        sizes.put(data, dataPages);
        sizes.put(pureData, pureDataSize);

        // Internal size.
        String internal = grpName + "-" + INTERNAL;

        DataStructureSize internalSize = simpleTracker(internal);

        sizes.put(internal, internalSize);

        // Index size.
        String indexesTotal = grpName + "-" + INDEX;

        DataStructureSize indexTotalPages = delegateWithTrackingPages(indexesTotal, internalSize, pageSize);

        sizes.put(indexesTotal, indexTotalPages);

        // Partitions size.
        String partitionTotal = grpName + "-" + PARTITION;

        DataStructureSize partitionTotalPages = simpleTracker(partitionTotal);

        sizes.put(partitionTotal, partitionTotalPages);

        // Total size.
        final String total = grpName + "-" + TOTAL;

        DataStructureSize totalPages = new DataStructureSizeAdapter() {
            @Override public long size() {
                return (indexTotalPages.size() + partitionTotalPages.size()) * pageSize + internalSize.size();
            }

            @Override public String name() {
                return total;
            }
        };

        sizes.put(total, totalPages);
    }

    private DataStructureSize dataStructureSize(String name) {
        return sizes.get(grpName + "-" + name);
    }

    public Map<String, DataStructureSize> structureSizes() {
        return sizes;
    }

    public static DataStructureSize delegateWithTrackingPages(
        String name,
        DataStructureSize internalSize,
        int pageSize
    ) {
        return new DataStructureSize() {
            private final long trackingPages = TrackingPageIO.VERSIONS.latest().countOfPageToTrack(pageSize);
            private final AtomicLong size = new AtomicLong();

            @Override public void inc() {
                long val = size.getAndIncrement();

                if (val % trackingPages == 0)
                    internalSize.add(pageSize);
            }

            @Override public void dec() {
                throw new UnsupportedOperationException();
            }

            @Override public void add(long val) {
                long prev = size.getAndAdd(val);

                if (prev / trackingPages < ((prev + val) / trackingPages))
                    internalSize.add(val * pageSize);
            }

            @Override public long size() {
                return size.get();
            }

            @Override public String name() {
                return name;
            }
        };
    }

    public static DataStructureSize simpleTracker(String name) {
        return new DataStructureSize() {
            private final AtomicLong size = new AtomicLong();

            @Override public void inc() {
                size.incrementAndGet();

               /* try {
                    PrintStream ps = System.err;

                    String msg = name + " " + size.get() + "\n";

                    ps.write(msg.getBytes());

                    new Exception().printStackTrace(ps);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }*/
            }

            @Override public void dec() {
                size.decrementAndGet();
            }

            @Override public void add(long val) {
                size.addAndGet(val);

           /*     try {
                    PrintStream ps = System.err;

                    String msg = name + " " + size.get() + " " + val + "\n";

                    ps.write(msg.getBytes());

                    new Exception().printStackTrace(ps);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }*/
            }

            @Override public long size() {
                return size.get();
            }

            @Override public String name() {
                return name;
            }
        };
    }

    public static DataStructureSize merge(DataStructureSize first, DataStructureSize second) {
        return new DataStructureSize() {

            @Override public void inc() {
                first.inc();

                second.inc();
            }

            @Override public void dec() {
                first.dec();

                second.dec();
            }

            @Override public void add(long val) {
                if (val != 0 && (val > 1 || val < -1)) {
                    first.add(val);

                    second.add(val);
                }

                if (val == -1)
                    dec();

                if (val == 1)
                    inc();
            }

            @Override public long size() {
                return first.size();
            }

            @Override public String name() {
                return first.name();
            }
        };
    }

    public static DataStructureSize delegateTracker(String name, DataStructureSize delegate) {
        return new DataStructureSize() {
            private final AtomicLong size = new AtomicLong();

            @Override public void inc() {
                size.incrementAndGet();

                if (delegate != null)
                    delegate.inc();
            }

            @Override public void dec() {
                size.decrementAndGet();

                if (delegate != null)
                    delegate.dec();
            }

            @Override public void add(long val) {
                if (val != 0 && (val > 1 || val < -1)) {
                    size.addAndGet(val);

                    if (delegate != null)
                        delegate.add(val);
                }

                if (val == -1)
                    dec();

                if (val == 1)
                    inc();
            }

            @Override public long size() {
                return size.get();
            }

            @Override public String name() {
                return name;
            }
        };
    }
}

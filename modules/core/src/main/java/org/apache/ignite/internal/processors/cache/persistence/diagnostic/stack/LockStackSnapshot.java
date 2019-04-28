package org.apache.ignite.internal.processors.cache.persistence.diagnostic.stack;

import java.text.SimpleDateFormat;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.Dump;
import org.apache.ignite.internal.util.typedef.internal.SB;

import static org.apache.ignite.internal.pagemem.PageIdUtils.flag;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.PageLockTracker.BEFORE_READ_LOCK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.PageLockTracker.BEFORE_WRITE_LOCK;
import static org.apache.ignite.internal.util.IgniteUtils.hexInt;
import static org.apache.ignite.internal.util.IgniteUtils.hexLong;

public class LockStackSnapshot implements Dump {
    private static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public final String name;

    public final long time;

    public final int headIdx;

    public final long[] pageIdLocksStack;

    public final int nextOp;
    public final int nextOpStructureId;
    public final long nextOpPageId;

    public LockStackSnapshot(
        String name,
        long time,
        int headIdx,
        long[] pageIdLocksStack,
        int nextOp,
        int nextOpStructureId,
        long nextOpPageId
    ) {
        this.name = name;
        this.time = time;
        this.headIdx = headIdx;
        this.pageIdLocksStack = pageIdLocksStack;
        this.nextOp = nextOp;
        this.nextOpStructureId = nextOpStructureId;
        this.nextOpPageId = nextOpPageId;
    }

    @Override public String toString() {
        SB res = new SB();

        res.a(name + " (time=" + time + ", " + DATE_FMT.format(new java.util.Date(time)) + ")")
            .a(" locked pages stack:\n");

        if (nextOpPageId != 0) {
            String str = "N/A";

            if (nextOp == BEFORE_READ_LOCK)
                str = "read lock";
            else if (nextOp == BEFORE_WRITE_LOCK)
                str = "write lock";

            res.a("\t-> try " + str + ", " + pageIdToString(nextOpPageId) + "\n");
        }

        for (int i = headIdx - 1; i >= 0; i--) {
            long pageId = pageIdLocksStack[i];

            if (pageId == 0 && i == 0)
                break;

            if (pageId == 0) {
                res.a("\t" + i + " -\n");
            }
            else {
                res.a("\t" + i + " " + pageIdToString(pageId) + "\n");
            }
        }

        res.a("\n");

        return res.toString();
    }

    private String pageIdToString(long pageId) {
        return "pageId=" + pageId
            + " [pageIdxHex=" + hexLong(pageId)
            + ", partId=" + pageId(pageId) + ", pageIdx=" + pageIndex(pageId)
            + ", flags=" + hexInt(flag(pageId)) + "]";
    }
}

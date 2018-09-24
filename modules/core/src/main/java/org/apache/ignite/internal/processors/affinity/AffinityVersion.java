package org.apache.ignite.internal.processors.affinity;

import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

public class AffinityVersion implements Comparable<AffinityVersion> {
    private final long majorVer;

    private final int minorVer;

    public AffinityVersion(long majorVer, int minorVer) {
        this.majorVer = majorVer;
        this.minorVer = minorVer;
    }

    public long majorVer() {
        return majorVer;
    }

    public int minorVer() {
        return minorVer;
    }

    /**
     * @return {@code True} if this is real affinity version.
     */
    public boolean initialized() {
        return majorVer > 0;
    }

    @Override public int compareTo(@NotNull AffinityVersion o) {
        int cmp = Long.compare(majorVer, o.majorVer);

        if (cmp == 0)
            cmp = Integer.compare(minorVer, o.minorVer);

        return cmp;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        AffinityVersion version = (AffinityVersion)o;

        return majorVer == version.majorVer && minorVer == version.minorVer;
    }

    @Override public int hashCode() {
        return Objects.hash(majorVer, minorVer);
    }

    @Override public String toString() {
        return S.toString(AffinityVersion.class, this);
    }
}

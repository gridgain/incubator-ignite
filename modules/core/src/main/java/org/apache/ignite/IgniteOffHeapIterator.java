package org.apache.ignite;

import org.apache.ignite.internal.util.GridUnsafe;

public class IgniteOffHeapIterator {

    long addr;

    int off;

    int len;

    public IgniteOffHeapIterator(long addr, int off, int len) {
        this.addr = addr;
        this.off = off;
        this.len = len;
    }

    public int remaining() {
        return 0;
    }

    public byte nextByte() {
        return GridUnsafe.getByte(addr + off++);
    }

    public int nextInt() {
        int anInt = GridUnsafe.getInt(addr + off);
        off += 4;
        return anInt;

    }
}

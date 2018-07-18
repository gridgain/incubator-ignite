package org.apache.ignite.internal.binary.compress;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public interface BinaryLookup<T> {

    class RootLookup<T> implements BinaryLookup<T> {
        private final BinaryLookup[] table;

        public RootLookup(BinaryLookup[] table) {
            assert table.length == 256;

            this.table = table;
        }

        public Entry<Integer, T> lookup(byte[] sequence, int pos) {
            return table[sequence[pos] & 0xff].lookup(sequence, pos);
        }
    }

    class ByteLookup<T> implements BinaryLookup<T> {
        private final Entry<Integer, T> entry;

        public ByteLookup(T val) {
            entry = new SimpleImmutableEntry<>(1, val);
        }

        public Entry<Integer, T> lookup(byte[] sequence, int pos) {
            return entry;
        }
    }

    class SequentialLookup<T> implements BinaryLookup<T> {
        private final byte[][] lookup;
        private final Entry[] results;

        public SequentialLookup(byte[][] lookup, Object[] values) {
            assert lookup.length == values.length;
            this.lookup = lookup;

            results = new Entry[lookup.length];

            for (int i = 0; i < lookup.length; i++)
                results[i] = new SimpleImmutableEntry(lookup[i].length + 1, values[i]);
        }

        public Entry<Integer, T> lookup(byte[] sequence, int pos) {
            //int bs = Arrays.binarySearch(
            //    lookup, sequence, new ByteArrayComparator(pos));
            for (int i = lookup.length - 1; i > 0; i--) {
                if (LZWBytes.startsWith(sequence, pos + 1, lookup[i]))
                    return results[i];
            }
            return results[0];
        }
    }

    class DichotomyLookup<T> implements BinaryLookup<T> {
        private final byte[][] lookup;
        private final Entry[] results;

        public DichotomyLookup(byte[][] lookup, Object[] values) {
            assert lookup.length == values.length;
            this.lookup = lookup;

            results = new Entry[lookup.length];

            for (int i = 0; i < lookup.length; i++)
                results[i] = new SimpleImmutableEntry(lookup[i].length + 1, values[i]);
        }

        public Entry<Integer, T> lookup(byte[] sequence, int pos) {
            //int bs = Arrays.binarySearch(
            //    lookup, sequence, new ByteArrayComparator(pos));
            int low = 1;
            int high = lookup.length - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                int cmp = ByteArrayComparator.INSTANCE.compare(sequence, pos + 1, lookup[mid], true);
                if (cmp > 0)
                    low = mid + 1;
                else if (cmp < 0)
                    high = mid - 1;
                else {
                    int retMid = mid;
                    while (cmp >= 0 && mid < high) {
                        cmp = ByteArrayComparator.INSTANCE.compare(sequence, pos + 1, lookup[mid + 1], true);
                        mid++;
                        if (cmp == 0) {
                            retMid = mid;
                        }
                    }

                    return results[retMid];
                }
            }

            for (int i = low - 1; i > 0; i--) {
                if (ByteArrayComparator.INSTANCE.compare(sequence, pos + 1, lookup[i], true) == 0)
                    return results[i];
            }

            return results[0];
        }
    }

    Entry<Integer, T> lookup(byte[] sequence, int pos);

    static <T> BinaryLookup<T> make(Map<byte[], T> dictionary, int minDich) {
        Entry[] entryArray = dictionary.entrySet().toArray(new Entry[0]);
        Arrays.sort(entryArray, (e1, e2) -> ByteArrayComparator.INSTANCE.compare(
            (byte[])e1.getKey(), (byte[])e2.getKey()));

        BinaryLookup[] table = new BinaryLookup[256];

        byte cur = (byte)0x80;
        List<Entry<byte[], T>> collected = new ArrayList();
        for (Entry<byte[], T> e : entryArray) {
            byte[] key = e.getKey();
            byte newCur = key[0];
            if (cur != newCur) {
                table[cur & 0xff] = makeLookup(collected, minDich);
                collected.clear();
                cur = newCur;
            }

            collected.add(e);
        }
        table[cur & 0xff] = makeLookup(collected, minDich);

        return new RootLookup<>(table);
    }

    static <T> BinaryLookup<T> makeLookup(List<Entry<byte[], T>> collected, int minDich) {
        assert collected.size() > 0;

        if (collected.size() == 1) {
            return new ByteLookup(collected.get(0).getValue());
        }
        else if (collected.size() <= minDich) {
            collected.sort((e1, e2) -> e1.getKey().length - e2.getKey().length);

            byte[][] lookup = new byte[collected.size()][];
            Object[] results = new Object[collected.size()];

            int i = 0;
            for (Entry<byte[], T> e : collected) {
                lookup[i] = Arrays.copyOfRange(e.getKey(), 1, e.getKey().length);
                results[i] = e.getValue();
                i++;
            }

            return new SequentialLookup<>(lookup, results);
        } else {
            collected.sort((e1, e2) -> ByteArrayComparator.INSTANCE.compare(e1.getKey(), e2.getKey()));

            byte[][] lookup = new byte[collected.size()][];
            Object[] results = new Object[collected.size()];

            int i = 0;
            for (Entry<byte[], T> e : collected) {
                lookup[i] = Arrays.copyOfRange(e.getKey(), 1, e.getKey().length);
                results[i] = e.getValue();
                i++;
            }

            return new DichotomyLookup<>(lookup, results);
        }
    }
}

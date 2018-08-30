package org.apache.ignite.internal.binary.compress;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteSystemProperties;

public class LZWBytes {

    public static final int DICTIONARY_SIZE = 16384;
    public static final int MIN_DELTA_BYTES = 8;
    private static final boolean CHECK = false;

    private final AtomicInteger dictSize = new AtomicInteger();
    private final TreeMap<byte[],Integer> dictionary = new TreeMap<>(ByteArrayComparator.INSTANCE);

    private volatile FastHuffmanDecoder<byte[]> decompressor;
    private volatile BinaryLookup<int[]> lookup;
    private volatile BinaryLookup<int[]> lookupForCheck;
    private final AtomicLong unc = new AtomicLong();
    private final AtomicLong comp = new AtomicLong();
    private final AtomicLong acc = new AtomicLong();
    private final AtomicLong rej = new AtomicLong();
    private final Lock dictLock = new ReentrantLock();
    private final boolean enabled = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_ENABLE_COMPRESSION);

    /** Compress a string to a list of output symbols. */
    public static void dictionarize(byte[] uncompressed, Map<byte[], Integer> dictionary, Lock lock) {
        if (uncompressed.length == 0 || !lock.tryLock())
            return;

        int lastpos = 0;
        for (int pos = 1; pos < uncompressed.length; pos++) {
            byte[] wc = Arrays.copyOfRange(uncompressed, lastpos, pos);
            if (dictionary.containsKey(wc))
                dictionary.compute(wc, (seq, dist) -> ++dist);
            else {
                dictionary.put(wc, pos - lastpos);
                lastpos = pos - 1;
            }
        }

        lock.unlock();
    }

    public byte[] handle(byte[] message) {
        BinaryLookup<int[]> lookup = this.lookup;

        if (true || lookup != null) {
            if (!enabled)
                return null;

            byte[] o = compress(message, lookup, false);

            if (CHECK) {
                if (ByteArrayComparator.INSTANCE.compare(o, compress(message, lookupForCheck, false)) != 0)
                    throw new IllegalStateException("Compressed did not match: "
                        + Arrays.toString(o) + " vs " + Arrays.toString(compress(message, lookupForCheck, false)));

                byte[] decompressed = decompress(o);
                if (ByteArrayComparator.INSTANCE.compare(message, decompressed) != 0)
                    throw new IllegalStateException("Recompressed did not match: "
                        + Arrays.toString(message) + " vs " + Arrays.toString(decompress(o)));

            }

            //System.err.println("Before: " + message.length + ", after: " + o.length);

            boolean accept = (message.length - o.length) > MIN_DELTA_BYTES;

            unc.addAndGet(message.length);
            comp.addAndGet(accept ? o.length : message.length);

            if (dictSize.incrementAndGet() % DICTIONARY_SIZE == DICTIONARY_SIZE - 1)
                System.out.println("Ratio: " + (float)comp.get() / (float)unc.get() +
                    ", acceptance: " + (acc.get() * 100L) / (rej.get() + acc.get()) + "%");

            if (accept) {
                acc.incrementAndGet();
                return o;
            } else {
                rej.incrementAndGet();
                return null;
            }
        }

        int iv = dictSize.incrementAndGet();
        if (iv < DICTIONARY_SIZE) {
            dictionarize(message, dictionary, dictLock);
        }
        else if (iv == DICTIONARY_SIZE) {
            dictLock.lock();
            System.out.println("Before prune: " + dictionary.size());
            prune(dictionary);
            System.out.println("After prune: " + dictionary.size());

            Entry[] entryArray = dictionary.entrySet().toArray(new Entry[0]);
            Arrays.sort(entryArray, (e1, e2) -> ((Comparable)e2.getValue()).compareTo(e1.getValue()));

            int[] freqs = new int[entryArray.length];
            byte[][] symbols = new byte[entryArray.length][];
            TreeMap<byte[], List<Integer>> forward = new TreeMap<>(ByteArrayComparator.INSTANCE);
            for (int c = 0; c < entryArray.length; c++) {
                freqs[c] = (int)entryArray[c].getValue();
                symbols[c] = (byte[])entryArray[c].getKey();
            }

            CodeTree codeTree = new FrequencyTable(freqs, symbols).buildCodeTree(forward);
            this.decompressor = new FastHuffmanDecoder<>(codeTree);

            Entry[] codesArray = ((Map<byte[], List<Integer>>)codeTree.codes).entrySet().toArray(new Entry[0]);

            Map<byte[], int[]> codesMap = new HashMap<>();

            for (Entry<byte[], List<Integer>> entry : codesArray) {
                List<Integer> value = entry.getValue();
                int[] encode = new int[2 + value.size() / 32];

                encode[0] = value.size();

                for (int i = 0; i < value.size(); i++) {
                    if (value.get(i) == 1)
                        encode[1 + i / 32] |= 1 << (i % 32);
                }

                codesMap.put(entry.getKey(), encode);
            }

            this.lookupForCheck = BinaryLookup.make(codesMap, 65536);
            this.lookup = BinaryLookup.make(codesMap, 32);

            dictLock.unlock();
        }

        return null;
    }

    /*private static byte[] compress(byte[] s, CodeTree codeTree, OffsetLookupTreeMap<byte[], ?> trie) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BitOutputStream bos = new BitOutputStream(out);
        HuffmanEncoder<byte[]> coder = new HuffmanEncoder<>(bos);
        coder.codeTree = codeTree;
        int pos = 0;
        while (pos < s.length) {
            byte[] substitute = trie.floorKey(s, pos);
            while (!startsWith(s, pos, substitute)) {
                substitute = trie.lowerKey(substitute);
            }

            try {
                //Integer symbol = trie.get(substitute);
                coder.write(substitute);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }

            pos += substitute.length;
        }

        try {
            bos.close();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        return out.toByteArray();
    }*/

    private static byte[] compress(byte[] s, BinaryLookup<int[]> trie, boolean b) {
        if (true)
            return Zstd.compress(s);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BitOutputStream bos = new BitOutputStream(out);
        int pos = 0;
        while (pos < s.length) {
            Entry<Integer, int[]> substitute = trie.lookup(s, pos);
            pos += substitute.getKey();

            try {
                int[] v = substitute.getValue();

                int numBits = v[0];
                for (int i = 1; i < v.length && numBits > 0; i++) {
                    int input = v[i];
                    bos.write(Math.min(32, numBits), input);
                    numBits -= 32;
                }
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        try {
            bos.close();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        return out.toByteArray();
    }

    public byte[] decompress(byte[] bytes) {
        return decompressor.decodeBinaryObject(bytes);
    }

    private static void prune(TreeMap<byte[], Integer> dictionary) {
        for (int i = 0; i < 256; i++) {
            byte[] c = {(byte)i};

            if (!dictionary.containsKey(c))
                dictionary.put(c, 1);
        }

        Iterator<Entry<byte[], Integer>> entries = dictionary.descendingMap().entrySet().iterator();
        byte[] previousKey = new byte[0];
        int previousValue = 0;
        while (entries.hasNext()) {
            Entry<byte[], Integer> entry = entries.next();
            if (entry.getKey().length == 1)
                continue;

            if (startsWith(previousKey, entry.getKey()) && previousValue + 1 >= entry.getValue()) {
                previousValue = Math.max(entry.getValue(), previousValue);
                entries.remove();
            }
            else if (entry.getKey().length * 2 > entry.getValue())
                entries.remove();

            else {
                previousKey = entry.getKey();
                previousValue = entry.getValue();
            }
        }
    }

    private static boolean startsWith(byte[] arr, byte[] part) {
        int i = 0;
        for (byte b : part) {
            if (arr.length == i || arr[i++] != b)
                return false;
        }
        return true;
    }

    static boolean startsWith(byte[] arr, int pos, byte[] part) {
        int i = pos;
        for (byte b : part) {
            if (arr.length <= i || arr[i++] != b)
                return false;
        }
        return true;
    }

}

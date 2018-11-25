package org.apache.ignite.internal.benchmarks.jmh.map;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.benchmarks.jmh.tree.BPlusTreeBenchmark;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.FullPageIdTable;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.LoadedPagesMap;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.RobinHoodBackwardShiftHashMap;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.RobinHoodBackwardShiftHashMapV2;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class RobinhoodHashMapBenchmark {
    private static final int KEYS = 1_000_000;

    private static final long MEMORY = 128 * 1024 * 1024;

    private DirectMemoryProvider memoryProvider;

    private LoadedPagesMap map;

    private int groupId;

    @Setup
    public void setup() {
        memoryProvider = new UnsafeMemoryProvider(null);

        long requiredMemory = MEMORY;

        memoryProvider.initialize(new long[] { requiredMemory });

        System.out.println(requiredMemory);

        DirectMemoryRegion region = memoryProvider.nextRegion();

        map = new RobinHoodBackwardShiftHashMapV2(region.address(), region.size());

        Random random = new Random();

        groupId = random.nextInt(KEYS);

        for (int k = 0; k < KEYS; k++) {
            map.put(groupId, random.nextLong(), random.nextInt(KEYS), 1);
        }
    }

    @TearDown
    public void teardown() {
        memoryProvider.shutdown(true);
    }

    @Benchmark
    public long get() {
        long key = ThreadLocalRandom.current().nextLong();

        long value = map.get(groupId, key, 1, 0, 0);

        //assert value != 0;

        return value;
    }

    public static void main (String[] args) throws Exception {
        JmhIdeBenchmarkRunner.create()
                .forks(1)
                .threads(1)
                .warmupIterations(5)
                .measurementIterations(10)
                .benchmarks(RobinhoodHashMapBenchmark.class.getSimpleName())
                .jvmArguments("-Xms4g", "-Xmx4g")
                .run();
    }
}

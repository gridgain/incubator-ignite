package org.apache.ignite;

import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {
        "-XX:+UseSuperWord",
        "-XX:+UnlockDiagnosticVMOptions"})
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class BenchmarkCRC
{
    public static final int SIZE = 1024;
    public static final int BUF_LEN = 4096;

    @State(Thread)
    public static class Context
    {
        public final int[] results = new int[SIZE];
        public final byte bb[] = new byte[BUF_LEN];

        @Setup
        public void setup() {
            for (int ii = 0; ii < BUF_LEN; ++ii)
                bb[ii] = (byte)ii;
        }
    }

    @Benchmark
    //@CompilerControl(CompilerControl.Mode.DONT_INLINE) //makes looking at assembly easier
    public int[] pureJavaCrc32(Context context)
    {
        PureJavaCrc32 crc = new PureJavaCrc32();

        ByteBuffer bb = ByteBuffer.wrap(context.bb);

        for (int i = 0; i < SIZE; i++) {
            bb.rewind();
            crc.update(bb, BUF_LEN);
            context.results[i] = crc.getValue();
        }

        return context.results;
    }

    @Benchmark
    //@CompilerControl(CompilerControl.Mode.DONT_INLINE) //makes looking at assembly easier
    public int[] Crc32(Context context)
    {
        CRC32 crc = new CRC32();

        ByteBuffer bb = ByteBuffer.wrap(context.bb); // for test eq purpose

        for (int i = 0; i < SIZE; i++) {
            bb.rewind(); // noop for test eq
            crc.update(bb);
            //crc.update(bb.array(), 0, BUF_LEN);
            context.results[i] = ((int)crc.getValue() ^ 0xFFFFFFFF) ;
        }
        return context.results;
    }
}



package org.apache.ignite.yardstick.streamer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteCallable;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkDriver;
import org.yardstickframework.BenchmarkProbe;
import org.yardstickframework.BenchmarkProbePoint;

/**  */
public class DataStorageMetricsProbe implements BenchmarkProbe {
    /** Collected points. */
    private Collection<BenchmarkProbePoint> collected = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public void start(BenchmarkDriver drv, BenchmarkConfiguration cfg) {
        collected = Collections.synchronizedList(new ArrayList<>());
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Collection<String> metaInfo() {
        return Arrays.asList(
            "Time, sec",
            "Last checkpoint duration, msec",
            "Last checkpoint lock wait duration, msec",
            "Last checkpoint total pages number",
            "Last checkpoint data pages number",
            "Last checkpoint copied on write pages number"
        );
    }

    /** {@inheritDoc} */
    @Override public synchronized Collection<BenchmarkProbePoint> points() {
        Collection<BenchmarkProbePoint> ret = collected;

        collected = new ArrayList<>();

        return ret;
    }

    /** {@inheritDoc} */
    @Override public void buildPoint(long time) {
        if (!Ignition.ignite().cluster().active())
            return;

        ClusterGroup oldest = Ignition.ignite().cluster().forOldest();
        double[] metrics = Ignition.ignite().compute(oldest).call(new MetricsJob());

        long seconds = TimeUnit.MILLISECONDS.toSeconds(time);

        BenchmarkProbePoint pnt = new BenchmarkProbePoint(seconds, metrics);

        collectPoint(pnt);
    }

    /**
     * @param pnt Probe point.
     */
    private synchronized void collectPoint(BenchmarkProbePoint pnt) {
        collected.add(pnt);
    }

    /** */
    private static class MetricsJob implements IgniteCallable<double[]> {
        /** */
        @Override public double[] call() {
            DataStorageMetrics metrics = Ignition.localIgnite().dataStorageMetrics();

            assert metrics != null;

            return new double[] {
                metrics.getLastCheckpointDuration(),
                metrics.getLastCheckpointLockWaitDuration(),
                metrics.getLastCheckpointTotalPagesNumber(),
                metrics.getLastCheckpointDataPagesNumber(),
                metrics.getLastCheckpointCopiedOnWritePagesNumber()
            };
        }
    }
}

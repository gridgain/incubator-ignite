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
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteCallable;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkDriver;
import org.yardstickframework.BenchmarkProbe;
import org.yardstickframework.BenchmarkProbePoint;

/**  */
public class DataRegionMetricsProbe implements BenchmarkProbe {
    /** Collected points. */
    private Collection<BenchmarkProbePoint> collected = new ArrayList<>();

    /** Last data collection time stamp. */
    private volatile long lastTstamp;

    /** Last total size value.*/
    private volatile double lastTotalSize;

    /** {@inheritDoc} */
    @Override public void start(BenchmarkDriver drv, BenchmarkConfiguration cfg) {
        collected = Collections.synchronizedList(new ArrayList<>());

        lastTstamp = System.currentTimeMillis();

        lastTotalSize = 0;
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Collection<String> metaInfo() {
        return Arrays.asList(
            "Time, sec",
            "Total allocated size, KB",
            "Number of dirty pages",
            "Checkpoint buffer size, KB",
            "Pages replace age, sec",
            "Pages replace rate, pages/sec",
            "Pages fill factor",
            "Total allocated size growth rate, KB/sec"
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

        long lastTstamp0 = lastTstamp;

        long lastTstamp1 = System.currentTimeMillis();

        lastTstamp = lastTstamp1;

        // Time delta in seconds, rounding is used because Thread.sleep(1000) can last less than a second.
        long delta = (long)Math.floor((lastTstamp1 - lastTstamp0) / 1000d + 0.5);

        ClusterGroup oldest = Ignition.ignite().cluster().forOldest();
        double[] metrics = Ignition.ignite().compute(oldest).call(new MetricsJob());
        List<Double> metricsList = DoubleStream.of(metrics).boxed().collect(Collectors.toList());

        double totalSize = metrics[0];
        double totalSizeGrowthRate = delta == 0
            ? Double.NaN
            : (totalSize - lastTotalSize) / delta;
        metricsList.add(totalSizeGrowthRate);

        lastTotalSize = totalSize;

        long seconds = TimeUnit.MILLISECONDS.toSeconds(time);

        BenchmarkProbePoint pnt = new BenchmarkProbePoint(seconds, metricsList.stream().mapToDouble(new ToDoubleFunction<Double>() {
            @Override public double applyAsDouble(Double d) {
                return d;
            }
        }).toArray());

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
            Ignite ignite = Ignition.localIgnite();
            String dfltRegionName = ignite.configuration()
                .getDataStorageConfiguration()
                .getDefaultDataRegionConfiguration()
                .getName();
            DataRegionMetrics metrics = ignite.dataRegionMetrics(dfltRegionName);

            assert metrics != null;

            return new double[] {
                metrics.getTotalAllocatedSize() / 1024,
                metrics.getDirtyPages(),
                metrics.getCheckpointBufferSize() / 1024,
                metrics.getPagesReplaceAge() / 1000,
                metrics.getPagesReplaceRate(),
                metrics.getPagesFillFactor()
            };
        }
    }
}

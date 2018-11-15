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

package org.apache.ignite.examples.ml.util.benchmark.thinclient.utils;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class Measure {
    private long latency;
    private long throughput;

    public Measure(long latency, long throughput) {
        this.latency = latency;
        this.throughput = throughput;
    }

    public long getLatency() {
        return latency;
    }

    public long getThroughput() {
        return throughput;
    }

    private static class Meta {
        private final String name;
        private final Function<Measure, Long> getter;
        private final String dim;

        public Meta(String name,
            Function<Measure, Long> getter, String dim) {
            this.name = name;
            this.getter = getter;
            this.dim = dim;
        }
    }

    public static void computeStatsAndPrint(long pageSize, List<Measure> measures) {
        List<Meta> measuresMetaToPrint = Arrays.asList(
            new Meta("latency", Measure::getLatency, "ms"),
            new Meta("throughput", Measure::getThroughput, "kbytes/s")
        );

        measuresMetaToPrint.forEach(t -> {
            double mean = mean(measures, t.getter);
            double stddev = std(measures, t.getter);
            double interval95 = 1.96 * stddev / Math.sqrt(measures.size());

            System.out.printf("Page size %d Mb, %s = %.2f ± %.2f %s\n", pageSize, t.name, mean, interval95, t.dim);
        });
    }

    private static double mean(Iterable<Measure> measures, Function<Measure, Long> getter) {
        long accumulatedValue = 0L;
        int counter = 0;

        for (Measure m : measures) {
            accumulatedValue += getter.apply(m);
            counter++;
        }

        return ((double)accumulatedValue) / counter;
    }

    private static double std(Iterable<Measure> measures, Function<Measure, Long> getter) {
        double mean = mean(measures, getter);

        long accumulatedValue = 0L;
        int counter = 0;

        for (Measure m : measures) {
            accumulatedValue += Math.pow(mean - getter.apply(m), 2);
            counter++;
        }

        return Math.sqrt(accumulatedValue / counter);
    }
}

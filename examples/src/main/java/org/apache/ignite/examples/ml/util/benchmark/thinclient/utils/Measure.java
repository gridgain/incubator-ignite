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
import java.util.stream.Collectors;

public class Measure {
    private static List<Meta> measuresMetaToPrint = Arrays.asList(
        new Meta("latency", Measure::getLatency, "ms"),
        new Meta("throughput", Measure::getThroughput, "kbytes/s")
    );

    private double latency;
    private double throughput;

    public Measure(double latency, double throughput) {
        this.latency = latency;
        this.throughput = throughput;
    }

    public static Measure sumOf(List<Measure> measures) {
        assert !measures.isEmpty();

        Measure sum = measures.stream()
            .reduce((left,right) -> new Measure(left.latency + right.latency, left.throughput + right.throughput))
            .get();
        return new Measure(sum.latency / measures.size(), sum.throughput);
    }

    public double getLatency() {
        return latency;
    }

    public double getThroughput() {
        return throughput;
    }

    private static class Meta {
        private final String name;
        private final Function<Measure, Double> getter;
        private final String dim;
        public Meta(String name,
            Function<Measure, Double> getter, String dim) {
            this.name = name;
            this.getter = getter;
            this.dim = dim;
        }

    }
    public static void computeStatsAndPrint(long pageSize, List<Measure> measures) {
        measuresMetaToPrint.forEach(t -> {
            MeasuringResult res = computeMetric(measures, t.getter);
            System.out.printf("Page size %d Mb, %s = %.2f ± %.2f %s\n", pageSize, t.name, res.mean, res.std, t.dim);
        });
    }

    public static List<MeasuringResult> computeAllMetrics(List<Measure> measures) {
        return measuresMetaToPrint.stream()
            .map(t -> computeMetric(measures, t.getter))
            .collect(Collectors.toList());
    }

    private static double mean(Iterable<Measure> measures, Function<Measure, Double> getter) {
        double accumulatedValue = 0L;
        int counter = 0;

        for (Measure m : measures) {
            accumulatedValue += getter.apply(m);
            counter++;
        }

        return accumulatedValue / counter;
    }

    private static double std(Iterable<Measure> measures, Function<Measure, Double> getter) {
        double mean = mean(measures, getter);

        long accumulatedValue = 0L;
        int counter = 0;

        for (Measure m : measures) {
            accumulatedValue += Math.pow(mean - getter.apply(m), 2);
            counter++;
        }

        return Math.sqrt(accumulatedValue / counter);
    }

    public static List<String> tableHeader() {
        return measuresMetaToPrint.stream().map(x -> x.name + "(" + x.dim + ")").collect(Collectors.toList());
    }

    private static MeasuringResult computeMetric(List<Measure> measures, Function<Measure, Double> getter) {
        double mean = mean(measures, getter);
        double stddev = std(measures, getter);
        double interval95 = 1.96 * stddev / Math.sqrt(measures.size());

        return new MeasuringResult(mean, interval95);
    }

    public static class MeasuringResult {
        public final double mean;
        public final double std;

        public MeasuringResult(double mean, double std) {
            this.mean = mean;
            this.std = std;
        }
    }
}

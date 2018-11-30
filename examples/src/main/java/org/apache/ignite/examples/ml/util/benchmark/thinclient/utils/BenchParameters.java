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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.util.Arrays;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class BenchParameters {
    @Parameter(names = "-qp", required = false, description = "query parallelism")
    private int queryParallelism = 2;
    @Parameter(names = "-coP", required = true, description = "cout of partitions, always > 0")
    private int countOfPartitions = 32;
    @Parameter(names = "-coR", required = true, description = "cout of rows, always > 0, always >= coP and coI")
    private int countOfRows = 500;
    @Parameter(names = "-sz", required = false, description = "value object size, always > 0")
    private int valueObjectSizeInBytes = 1024 * 1024;
    @Parameter(names = "-mtc", required = true, description = "max thin clients, always > 0, always <= coP")
    private int maxThreadCount = 16;
    @Parameter(names = "-coI", required = true, description = "count of ignites, always > 0")
    private int countOfIgnites = 1;
    @Parameter(names = "-h", required = false, description = "view help")
    private boolean help = false;

    public static BenchParameters parseArguments(String ... args) {
        BenchParameters parameters = new BenchParameters();
        JCommander jc = new JCommander(parameters);
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.out.println("current argument tokens: " + Arrays.stream(args).collect(Collectors.joining(",")));
            jc.usage();
            System.exit(0);
        }

        if(parameters.help) {
            jc.usage();
            System.exit(0);
        }

        assertion(parameters.countOfPartitions > 0, "COUNT_OF_PARTITIONS > 0");
        assertion(parameters.maxThreadCount > 0, "MAX_THREAD_COUNT > 0");
        assertion(parameters.queryParallelism > 0,"QUERY_PARALLELISM > 0");
        assertion(parameters.valueObjectSizeInBytes > 0, "VALUE_OBJECT_SIZE_IN_BYTES > 0");

        assertion(parameters.countOfPartitions >= parameters.countOfIgnites, "COUNT_OF_PARTITIONS >= COUNT_OF_IGNITES");
        assertion(parameters.countOfRows >= parameters.countOfPartitions, "COUNT_OF_ROWS >= COUNT_OF_PARTITIONS");
        assertion(parameters.maxThreadCount <= parameters.countOfPartitions, "MAX_THREAD_COUNT <= COUNT_OF_PARTITIONS");

        return parameters;
    }

    private static void assertion(boolean pred, String msg) {
        if(!pred)
            throw new RuntimeException(msg);
    }

    public int getQueryParallelism() {
        return queryParallelism;
    }

    public void setQueryParallelism(int queryParallelism) {
        this.queryParallelism = queryParallelism;
    }

    public int getCountOfPartitions() {
        return countOfPartitions;
    }

    public void setCountOfPartitions(int countOfPartitions) {
        this.countOfPartitions = countOfPartitions;
    }

    public int getCountOfRows() {
        return countOfRows;
    }

    public void setCountOfRows(int countOfRows) {
        this.countOfRows = countOfRows;
    }

    public int getValueObjectSizeInBytes() {
        return valueObjectSizeInBytes;
    }

    public void setValueObjectSizeInBytes(int valueObjectSizeInBytes) {
        this.valueObjectSizeInBytes = valueObjectSizeInBytes;
    }

    public int getMaxThreadCount() {
        return maxThreadCount;
    }

    public void setMaxThreadCount(int maxThreadCount) {
        this.maxThreadCount = maxThreadCount;
    }

    public int getCountOfIgnites() {
        return countOfIgnites;
    }

    public void setCountOfIgnites(int countOfIgnites) {
        this.countOfIgnites = countOfIgnites;
    }

    @Override public String toString() {
        return "query parallelism = " + queryParallelism +
            ", count of partitions = " + countOfPartitions +
            ", count of rows = " + countOfRows +
            ", value object size in bytes = " + valueObjectSizeInBytes +
            ", max thread count = " + maxThreadCount +
            ", count of ignites = " + countOfIgnites;
    }
}

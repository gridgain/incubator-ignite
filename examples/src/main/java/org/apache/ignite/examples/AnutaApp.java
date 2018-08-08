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

package org.apache.ignite.examples;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import javax.cache.Cache;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Starts up an empty node with example compute configuration.
 */
public class AnutaApp {
    /**
     * Start up an empty node with example compute configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/anuta-ignite-simple.xml")) {
            Cache<String, String> cache = ignite.cache("task_execution_data");

            final int TX_CNT = 100;
            final int ENTRY_CNT = 10_000;

            for (int t = 0; t < TX_CNT; t++) {
                BiFunction<Integer, Integer, String> getKey = (n1, n2) -> String.format("%s-%s", n1, n2);

                try (Transaction ignored = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE)) {
                    for (int e = 0; e < ENTRY_CNT; e++)
                        cache.put(getKey.apply(t, e), UUID.randomUUID().toString());
                }

                try (Transaction ignored = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    for (int e = 0; e < ENTRY_CNT; e++)
                        cache.get(getKey.apply(t, e));
                }

                System.out.format("TX %s\n", t);
            }
        }
    }
}

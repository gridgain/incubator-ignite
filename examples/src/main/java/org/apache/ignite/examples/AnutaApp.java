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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.sun.corba.se.impl.util.RepositoryId.cache;

/**
 * Starts up an empty node with example compute configuration.
 */
public class AnutaApp {
    private static final int TX_CNT = 100;
    private static final int ENTRY_CNT = 1000;

    /**
     * Start up an empty node with example compute configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If failed.
     */
    public static void main(String[] args) throws IgniteException, InterruptedException {
        try (Ignite ignite = Ignition.start("examples/config/anuta-ignite-simple.xml")) {
            Cache<String, String> cache = ignite.cache("task_execution_data");
            ExecutorService executorSvc = Executors.newFixedThreadPool(10);

            for (int t = 0; t < TX_CNT; t++) {
                executorSvc.execute(() -> {
                    try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE)) {
                        for (int e = 0; e < ENTRY_CNT; e++) {
                            String key = Integer.toString(e);
                            if (!cache.containsKey(key))
                                cache.put(key, UUID.randomUUID().toString());
                        }

                        tx.commit();
                    }
                });

                executorSvc.execute(() -> {
                    try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                        for (int e = 0; e < ENTRY_CNT; e++) {
                            String key = Integer.toString(e);
                            if (cache.containsKey(key))
                                cache.remove(key);
                        }

                        tx.commit();
                    }
                });

                System.out.format("TX %s\n", t);
            }

            executorSvc.awaitTermination(10, TimeUnit.MINUTES);
        }
    }
}
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

package org.apache.ignite.yardstick.cache;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.yardstick.cache.model.Person1;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs SQL INSERT operations for entity with indexed fields.
 */
public class IgniteSqlInsertIndexedValue1Benchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** */
    private final AtomicInteger insItemsCnt = new AtomicInteger();

    /** */
    private final AtomicInteger insCnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int key = nextRandom(args.range());

        int res = (Integer) cache.query(new SqlFieldsQuery("insert into Person1(_key, _val) values (?, ?)")
            .setArgs(key, new Person1(key)).setSkipDuplicateKeys(true)).getAll().get(0).get(0);

        insItemsCnt.addAndGet(res);

        insCnt.incrementAndGet();

        return true;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        println(cfg, "Finished SQL INSERT query benchmark [insItemsCnt=" + insItemsCnt.get() + ", insCnt=" +
            insCnt.get() + ']');

        super.tearDown();
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic-index");
    }
}

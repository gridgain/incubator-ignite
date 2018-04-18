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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.util.typedef.X;

import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.connect;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.execute;

/**
 * Test for {@code SELECT FOR UPDATE} queries.
 */
public class SelectForUpdateQueryTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(3);

        try (Connection c = connect(grid(0))) {
            execute(c, "create table person (id int primary key, firstName varchar, lastName varchar) " +
                "with \"atomicity=transactional,cache_name=Person\"");

            for (int i = 1; i <= 300; i++)
                execute(c, "insert into person(id, firstName, lastName) values(" + i + ",'" + i + "','"  + i + "')");
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try (Connection c = connect(grid(0))) {
            execute(c, "drop table if exists person");
        }

        super.afterTest();
    }

    /**
     *
     */
    public void testSelectForUpdate() throws IgniteCheckedException {
        IgniteEx node = grid(0);

        IgniteCache<Integer, ?> cache = node.cache("Person");

        cache.query(new SqlFieldsQuery("begin"));

        try {
            SqlFieldsQuery qry = new SqlFieldsQuery("select id from person order by id for update").setPageSize(10);

            List<List<?>> res = cache.query(qry).getAll();

            assertEquals(300, res.size());

            GridNearTxLocal tx = MvccUtils.activeTx(node.context());

            assertNotNull(tx);

            IgniteInternalCache<Integer, ?> cachex = node.cachex("Person").keepBinary();

            for (Cache.Entry e : cachex.localEntries(new CachePeekMode[]{ CachePeekMode.PRIMARY })) {
                assertNotNull(tx.txState().writeMap()
                    .get(new IgniteTxKey(cachex.context().toCacheKeyObject(e.getKey()), cachex.context().cacheId())));
            }
        }
        finally {
            node.cache("Person").query(new SqlFieldsQuery("rollback"));
        }
    }

    /**
     *
     */
    public void testSelectForUpdateLocal() {
        Ignite node = grid(0);

        node.cache("Person").query(new SqlFieldsQuery("begin"));

        SqlFieldsQuery qry = new SqlFieldsQuery("select id from person order by id for update").setLocal(true);

        List<List<?>> res = node.cache("Person").query(qry).getAll();

        Set<Integer> keys = new HashSet<>();

        for (List<?> r : res)
            X.println(r.get(0).toString());

        node.cache("Person").query(new SqlFieldsQuery("rollback"));
    }
}

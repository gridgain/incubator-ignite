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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.util.typedef.X;

import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.connect;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.execute;

/**
 * Test for {@code SELECT FOR UPDATE} queries.
 */
public class SelectForUpdateQueryTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMvccEnabled(true);

        CacheConfiguration ccfg = new CacheConfiguration("mvcc*");

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        return cfg;
    }

    /**
     *
     */
    public void testSelectForUpdate() throws SQLException {
        try (Connection c = connect(grid(0))) {
            c.setAutoCommit(false);

            try (PreparedStatement ps = c.prepareStatement("select * from person  order by id for update")) {
                ps.setFetchSize(10);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next())
                        X.println(rs.getObject(1).toString());
                }
            }
        }
    }

    /**
     *
     */
    public void testSelectForUpdateLocal() throws SQLException {
        Ignite node = ignite(0);

        SqlFieldsQueryEx qry = new SqlFieldsQueryEx("select * from person order by id for update", true).setLocal(true);

        qry.setAutoCommit(false);

        List<List<?>> res = node.cache("Person").query(qry).getAll();

        for (List<?> r : res)
            X.println(r.get(0).toString());
    }
}

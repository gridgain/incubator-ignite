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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 *
 */
public class JdbcThinSchemaCaseTest extends JdbcThinAbstractSelfTest {
    /** URL. */
    private String url = bestEffortAffinity ?
        "jdbc:ignite:thin://127.0.0.1:10800..10802" :
        "jdbc:ignite:thin://127.0.0.1";

    /** Nodes count. */
    private int nodesCnt = bestEffortAffinity ? 4 : 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(
            cacheConfiguration("test0", "test0"),
            cacheConfiguration("test1", "tEst1"),
            cacheConfiguration("test2", "\"TestCase\""));

        return cfg;
    }

    /**
     * @param name Cache name.
     * @param schema Schema name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheConfiguration(@NotNull String name, @NotNull String schema) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setIndexedTypes(Integer.class, Integer.class);

        cfg.setName(name);

        cfg.setSqlSchema(schema);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(nodesCnt);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"unused"})
    @Test
    public void testSchemaName() throws Exception {
        checkSchemaConnection("test0");
        checkSchemaConnection("test1");
        checkSchemaConnection("\"TestCase\"");
        checkSchemaConnection("\"TEST0\"");
        checkSchemaConnection("\"TEST1\"");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                checkSchemaConnection("TestCase");

                return null;
            }
        }, SQLException.class, null);
    }

    /**
     * @param schema Schema name.
     * @throws SQLException If failed.
     */
    void checkSchemaConnection(String schema) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url + '/' + schema)) {
            Statement stmt = conn.createStatement();

            assertNotNull(stmt);
            assertFalse(stmt.isClosed());

            stmt.execute("select t._key, t._val from Integer t");
        }
    }
}

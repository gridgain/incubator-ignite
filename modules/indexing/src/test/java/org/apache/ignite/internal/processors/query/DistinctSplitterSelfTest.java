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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Tests for distinct queries.
 */
public class DistinctSplitterSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(2);
    }

    public void testQuery() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE client (id BIGINT PRIMARY KEY, name VARCHAR)");
                stmt.execute("CREATE TABLE good (id BIGINT PRIMARY KEY, name VARCHAR)");
                stmt.execute("CREATE TABLE ord (id BIGINT PRIMARY KEY, cliId BIGINT, goodId BIGINT)");

//                stmt.executeQuery(
//                    "SELECT cli.name, good.name " +
//                    "FROM (SELECT DISTINCT cliId, goodId FROM ord) as ord " +
//                    "    LEFT JOIN client cli ON cli.id = ord.cliId " +
//                    "    JOIN good good ON good.id = ord.goodId"
//                ).close();

                stmt.executeQuery(
                    "SELECT COUNT(c.name) FROM client c JOIN ord o ON c.id = o.cliId"
                ).close();
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }
}

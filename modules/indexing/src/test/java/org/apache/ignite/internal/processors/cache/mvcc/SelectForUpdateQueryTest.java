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
import java.sql.SQLException;

import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.connect;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.execute;

/**
 * Test for {@code SELECT FOR UPDATE} queries.
 */
public class SelectForUpdateQueryTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);
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
                "with \"atomicity=transactional\"");
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
    public void testSelectForUpdate() throws SQLException {
        try (Connection c = connect(grid(0))) {
            for (int i = 1; i <= 300; i++)
             execute(c, "insert into person(id, firstName, lastName) values(" + i + ",'" + i + "','"  + i + "')");
        }

        try (Connection c = connect(grid(0))) {
            c.setAutoCommit(false);

            try (PreparedStatement ps = c.prepareStatement("select * from person for update")) {
                ps.setFetchSize(10);

                ps.execute();
            }
        }
    }
}


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
 *
 */

package org.apache.ignite.compatibility.persistence;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.lang.IgniteInClosure;
import org.junit.Test;

/**
 * Test to check that starting node with PK index of the old format present doesn't break anything.
 */
public class IgnitePKIndexesMigrationToAlternativeKeyTest extends IndexingMigrationAbstractionTest {
    /** */
    private static final String TABLE_NAME = "TEST_IDX_TABLE";

    /** */
    private static final String COMP_SUFFIX = "_compPK";

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSecondaryIndexesMigration_2_7() throws Exception {
        doTestStartupWithOldVersion("2.7.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSecondaryIndexesMigration_2_5() throws Exception {
        doTestStartupWithOldVersion("2.5.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSecondaryIndexesMigration_2_4() throws Exception {
        doTestStartupWithOldVersion("2.4.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @param ver 3-digits version of ignite
     * @throws Exception If failed.
     */
    private void doTestStartupWithOldVersion(String ver) throws Exception {
        try {
            startGrid(1, ver, new ConfigurationClosure(), new PostStartupClosure(true));

            stopAllGrids();

            IgniteEx igniteEx = startGrid(0);

            new PostStartupClosure(true).apply(igniteEx);

            igniteEx.cluster().active(true);

            assertDontUsingPkIndex(igniteEx, TABLE_NAME);

            String newTblName = TABLE_NAME + "_NEW";

            initializeTable(igniteEx, newTblName);

            checkUsingIndexes(igniteEx, newTblName);

            igniteEx.cluster().active(false);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** */
        boolean createTable;

        /**
         * @param createTable {@code true} In case table should be created
         */
        public PostStartupClosure(boolean createTable) {
            this.createTable = createTable;
        }

        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.cluster().active(true);

            IgniteEx igniteEx = (IgniteEx)ignite;

            if (createTable)
                initializeTable(igniteEx, TABLE_NAME);

            assertDontUsingPkIndex(igniteEx, TABLE_NAME);

            checkUsingIndexes(igniteEx, TABLE_NAME);

            ignite.cluster().active(false);
        }
    }

    /**
     * @param igniteEx Ignite instance.
     * @param tblName  Table name.
     */
    private static void initializeTable(IgniteEx igniteEx, String tblName) {
        initializeTable(igniteEx, tblName, true);

        initializeTable(igniteEx, tblName, false);
    }

    /**
     * @param igniteEx Ignite instance.
     * @param tblName Table name.
     * @param compPK Compound PK.
     */
    private static void initializeTable(IgniteEx igniteEx, String tblName, boolean compPK) {
        int ITEMS = 500;

        tblName += compPK ? COMP_SUFFIX : "";

        String pk = compPK ? "(id, name, city)" : "(name)";

        executeSql(igniteEx, "CREATE TABLE IF NOT EXISTS " + tblName + " (id int, name varchar, age int, company " +
            "varchar, city varchar, primary key " + pk + ") WITH \"affinity_key=name\"");

        executeSql(igniteEx, "CREATE INDEX IF NOT EXISTS \"my_idx_" + tblName + "\" ON " + tblName + "(city, id)");

        List<List<?>> res = executeSql(igniteEx, "select max(id) from " + tblName);

        Object data = res.get(0).get(0);

        int mltpl = data == null ? 0 : 1;

        for (int i = ITEMS * mltpl; i < ITEMS * (mltpl + 1); i++)
            executeSql(igniteEx, "INSERT INTO " + tblName + " (id, name, age, company, city) VALUES (" + i + ",'name" +
                i + "', 2, 'company', 'city" + i + "')");

        if (mltpl != 0) {
            res = executeSql(igniteEx, "select max(id) from " + tblName);

            data = res.get(0).get(0);

            assertTrue(data != null && (int)data > ITEMS);
        }
    }

    /**
     * Run SQL statement on specified node.
     *
     * @param node node to execute query.
     * @param stmt Statement to run.
     * @param args arguments of statements
     * @return Run result.
     */
    private static List<List<?>> executeSql(IgniteEx node, String stmt, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(stmt).setArgs(args), true).getAll();
    }

    /**
     * Check using PK indexes for few cases.
     *
     * @param ignite Ignite instance.
     * @param tblName name of table which should be checked to using PK indexes.
     */
    private static void checkUsingIndexes(IgniteEx ignite, String tblName) {
        checkUsingIndexes(ignite, tblName, true);

        checkUsingIndexes(ignite, tblName, false);
    }

    /**
     * Check using PK indexes for few cases.
     *
     * @param ignite Ignite instance.
     * @param tblName name of table which should be checked to using PK indexes.
     * @param compPK Compound PK.
     */
    private static void checkUsingIndexes(IgniteEx ignite, String tblName, boolean compPK) {
        tblName += compPK ? COMP_SUFFIX : "";

        List<List<?>> res = executeSql(ignite, "select max(id) from " + tblName);

        Object data = res.get(0).get(0);

        assertTrue("No data found!", data != null);

        for (int i = 0; i < (int)data; i++) {
            String s = "explain SELECT name FROM " + tblName + " WHERE city='" + i + "' AND id=0 AND name='name" + i + "'";

            List<List<?>> results = executeSql(ignite, s);

            String explainPlan = (String)results.get(0).get(0);

            System.err.println("explain: " + explainPlan);
        }
    }

    /**
     * Check that explain plan result shown don't use PK index and use scan.
     *
     * @param igniteEx Ignite instance.
     * @param tblName Name of table.
     */
    private static void assertDontUsingPkIndex(IgniteEx igniteEx, String tblName) {
        List<List<?>> results = executeSql(igniteEx, "explain SELECT * FROM " + tblName + " WHERE id=1");

        assertEquals(2, results.size());

        String explainPlan = (String)results.get(0).get(0);

        assertFalse(explainPlan, explainPlan.contains(H2TableDescriptor.PK_IDX_NAME));

        assertTrue(explainPlan, explainPlan.contains("_SCAN_"));
    }

}

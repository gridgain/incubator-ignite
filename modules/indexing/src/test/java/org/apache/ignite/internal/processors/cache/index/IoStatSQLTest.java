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

package org.apache.ignite.internal.processors.cache.index;

import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryStatistics;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.stat.GridIoStatManager;
import org.apache.ignite.internal.stat.StatType;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

;

/**
 * A set of basic tests for caches with indexes.
 */
public class IoStatSQLTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES_COUNT = 2;

    /** */
    private static final int ORG_COUNT = NODES_COUNT;

    /** */
    private static final int PERSON_PER_ORG_COUNT = 50;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi().setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_COUNT, false);
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param transactional {@code true} In case test should be transactional.
     * @param idxTypes Indexed types.
     * @return Cache configuration.
     * @param <K> Key type.
     * @param <V> Value type.
     */
    protected <K, V> CacheConfiguration<K, V> cacheConfig(String name, boolean partitioned, boolean transactional,
        Class<?>... idxTypes) {
        return new CacheConfiguration<K, V>(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setAtomicityMode(transactional ? CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT : CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setIndexedTypes(idxTypes);
    }

    /**
     * Test gathering IO statistics for local query.
     *
     * @throws Exception If failed.
     */
    public void testSimpleLocalQuery() throws Exception {
        simpleLocalQueryTest(false);
    }

    /**
     * Test gathering IO statistics for local query within transaction.
     *
     * @throws Exception If failed.
     */
    public void testSimpleLocalQueryTransctional() throws Exception {
        simpleLocalQueryTest(true);
    }

    /**
     * @param transactional {@code true} In case test should be transactional.
     * @throws Exception In case of Failure.
     */
    private void simpleLocalQueryTest(boolean transactional) throws Exception {
        CacheConfiguration ccfg1 = cacheConfig("pers", false, transactional, String.class, Person.class);

        IgniteEx ignite = ((IgniteEx)ignite(0));

        IgniteCache<String, Person> c1 = ignite.getOrCreateCache(ccfg1);

        GridIoStatManager ioStatMgr = ignite.context().ioStats();

        try {
            awaitPartitionMapExchange();

            populateDataIntoCache(c1);

            String sql = "select * from Person where lower(name) = lower(?)";

            SqlFieldsQuery qry = new SqlFieldsQuery(sql).setArgs("Person name #2").setLocal(true);

            Transaction tx = null;

            if (transactional)
                tx = ignite(0).transactions().txStart();

            ioStatMgr.resetStats();

            FieldsQueryCursor qryCursor = c1.query(qry);
            qryCursor.getAll();

            QueryStatistics stats = qryCursor.stats();

            assertEquals(calculateOverallLogicalReads(ioStatMgr), stats.logicalReads());

            assertEquals(0, stats.physicalReads());

            if (transactional)
                tx.commit();
        }
        finally {
            c1.destroy();
        }
    }

    /**
     * Test gathering IO statistics non collocated distributed join for small page size and case when result can fit
     * into one page.
     *
     * @throws Exception If failed.
     */
    public void testNonCollocatedDistributedJoin() throws Exception {
        nonCollocatedDistributedJoinTest(false);
    }

    /**
     * Test gathering IO statistics non collocated distributed join for small page size and case when result can fit
     * into one page within transaction.
     *
     * @throws Exception In case of failure.
     */
    public void testNonCollocatedDistributedTransactionalJoin() throws Exception {
        nonCollocatedDistributedJoinTest(true);
    }

    /**
     * @param transactional {@code true} In case test should be transactional.
     * @throws Exception In case of failure.
     */
    private void nonCollocatedDistributedJoinTest(boolean transactional) throws Exception {
        CacheConfiguration<String, Person> ccfg1 = cacheConfig("pers", true, transactional, String.class, Person.class);
        CacheConfiguration<String, Organization> ccfg2 = cacheConfig("org", true, transactional, String.class, Organization.class);

        IgniteEx ignite = ((IgniteEx)ignite(0));
        IgniteEx ignite2 = ((IgniteEx)ignite(1));

        IgniteCache<String, Person> c1 = ignite.getOrCreateCache(ccfg1);
        IgniteCache<String, Organization> c2 = ignite.getOrCreateCache(ccfg2);

        GridIoStatManager ioStatMgr = ignite.context().ioStats();
        GridIoStatManager ioStatMgr2 = ignite2.context().ioStats();

        awaitPartitionMapExchange();

        try {
            populateDataIntoCaches(c1, c2);

            String joinSql =
                "select * from Person, \"org\".Organization as org " +
                    "where Person.orgId = org.id " +
                    "and lower(org.name) = lower(?)";

            Stream.of(1, PERSON_PER_ORG_COUNT + 1).forEach(pageSize -> {

                    SqlFieldsQuery qry = new SqlFieldsQuery(joinSql).setArgs("Organization #0")
                        .setDistributedJoins(true).setPageSize(pageSize);

                    Transaction tx = null;

                    if (transactional)
                        tx = ignite(0).transactions().txStart();

                ioStatMgr.resetStats();

                ioStatMgr2.resetStats();

                    FieldsQueryCursor qryCursor = c1.query(qry);

                    assertEquals(PERSON_PER_ORG_COUNT, qryCursor.getAll().size());

                    QueryStatistics stats = qryCursor.stats();

                long overallLogicalReads = calculateOverallLogicalReads(ioStatMgr, ioStatMgr2);

                    assertEquals(overallLogicalReads, stats.logicalReads());

                    assertEquals(0, stats.physicalReads());

                    if (transactional)
                        tx.commit();

                }
            );

        }
        finally {
            c1.destroy();
            c2.destroy();
        }
    }

    /**
     * @param c1 Cache1.
     * @param c2 Cache2.
     */
    private void populateDataIntoCaches(IgniteCache<String, Person> c1, IgniteCache<String, Organization> c2) {
        int personId = 0;

        for (int i = 0; i < ORG_COUNT; i++) {
            Organization org = new Organization();
            org.setId("org" + i);
            org.setName("Organization #" + i);

            c2.put(org.getId(), org);

            for (int j = 0; j < PERSON_PER_ORG_COUNT; j++) {
                Person prsn = new Person();
                prsn.setId("pers" + personId);
                prsn.setOrgId(org.getId());
                prsn.setName("Person name #" + personId);

                c1.put(prsn.getId(), prsn);

                personId++;
            }
        }
    }

    /**
     * @param cache Cache
     */
    private void populateDataIntoCache(IgniteCache<String, Person> cache) {
        for (int personId = 0; personId < PERSON_PER_ORG_COUNT; personId++) {
            Person prsn = new Person();

            prsn.setId("pers" + personId);

            prsn.setName("Person name #" + personId);

            cache.put(prsn.getId(), prsn);
        }
    }

    /**
     * @param ioStatManagers IO statistic managers.
     * @return overall logical reads.
     */
    private long calculateOverallLogicalReads(GridIoStatManager... ioStatManagers) {
        long totalLogicalReads = 0;

        for (GridIoStatManager ioStatManager : ioStatManagers) {

            for (StatType statType : StatType.values()) {
                Set<String> statNames = ioStatManager.deriveStatNames(statType);

                for (String name : statNames) {
                    Set<String> statSubNames = ioStatManager.deriveStatSubNames(statType, name);

                    for (String subName : statSubNames) {
                        Long logicalReads = ioStatManager.logicalReads(statType, name, subName);

                        if (logicalReads != null)
                            totalLogicalReads += logicalReads;
                    }

                    Long logicalReads = ioStatManager.logicalReads(statType, name);

                    if (logicalReads != null)
                        totalLogicalReads += logicalReads;
                }
            }
        }

        return totalLogicalReads;
    }

    /**
     *
     */
    private static class Person {
        /** */
        @QuerySqlField(index = true)
        private String id;

        /** */
        @QuerySqlField(index = true)
        private String orgId;

        /** */
        @QuerySqlField(index = true)
        private String name;

        /**
         * @return id.
         */
        public String getId() {
            return id;
        }

        /**
         * @param id Id.
         */
        public void setId(String id) {
            this.id = id;
        }

        /**
         * @return Organization id.
         */
        public String getOrgId() {
            return orgId;
        }

        /**
         * @param orgId Organization id.
         */
        public void setOrgId(String orgId) {
            this.orgId = orgId;
        }

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }
    }

    /**
     *
     */
    private static class Organization {
        /** */
        @QuerySqlField(index = true)
        private String id;

        /** */
        @QuerySqlField(index = true)
        private String name;

        /**
         * @param id Id.
         */
        public void setId(String id) {
            this.id = id;
        }

        /**
         * @return Id.
         */
        public String getId() {
            return id;
        }

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }
    }
}

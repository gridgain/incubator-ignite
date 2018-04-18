/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_CONNECTION_FAILED;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Command line handler test for transactions dumping
 */
public class GridCommandHandlerTxTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE1 = "CACHE1";

    /** */
    private final ByteArrayOutputStream out = new ByteArrayOutputStream(1024);

    /** */
    private final ByteArrayOutputStream err = new ByteArrayOutputStream(1024);

    /** */
    private String outStr;

    /** Client mode. */
    private boolean client;

    /**  */
    private int val;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setOut(new PrintStream(out));

        System.setErr(new PrintStream(err));

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        System.setOut(new PrintStream(System.out));

        System.setErr(new PrintStream(System.err));

        String str = out.toString();

        if (!F.isEmpty(str))
            System.out.println(str);

        str = err.toString();

        if (!F.isEmpty(str))
            System.err.println(str);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(client);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        cfg.setCacheConfiguration(getCacheConfiguration());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration[] getCacheConfiguration() {
        return new CacheConfiguration[] {
            new CacheConfiguration(CACHE1)
                .setCacheMode(CacheMode.PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(2)
                .setAffinity(new RendezvousAffinityFunction())

        };
    }

    /**
     * @param args Arguments.
     * @return Execution code.
     */
    private int execute(String... args) {
        resetOutput();

        List<String> argList = new ArrayList<>(Arrays.asList(args));
        argList.add("--force");

        int res = new CommandHandler().execute(argList);

        outStr = out.toString();

        return res;
    }

    /**
     * @param args Arguments.
     */
    private void checkExecute(String... args) {
        int res = execute(args);

        assertEquals(outStr, EXIT_CODE_OK, res);
    }

    /**
     * @param args Arguments.
     */
    private void checkExecute(int exitCode, String... args) {
        int res = execute(args);

        assertEquals(outStr, exitCode, res);
    }

    /**
     * test transactions dump
     */
    public void testTx() throws Exception {
        checkExecute(EXIT_CODE_CONNECTION_FAILED, "--tx", "dur", "0");

        int SRVS = 2;
        int CLIENTS = 2;

        startGrids(SRVS);

        client = true;

        startGridsMultiThreaded(SRVS, CLIENTS);

        client = false;

        checkExecute("--tx", "dur", "0");

        assertTrue(outStr, outStr.contains("No active transactions found"));

        IgniteEx client1 = grid(SRVS);
        IgniteEx client2 = grid(SRVS + 1);

        assertTrue(client1.configuration().isClientMode());

        try (Transaction tx = startTx(client1, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED, "tx-label-1");
             Transaction tx2 = startTx(client2, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED, "tx-label-2")) {
            checkExecute("--tx", "dur", "0");

            assertFalse(outStr, outStr.contains("No active transactions found"));

            assertTrue(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-label-1")));

            assertTrue(outStr, outStr.contains(String.valueOf(tx2.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-label-2")));

            tx.commit();
        }

        checkExecute("--tx", "dur", "0");

        assertTrue(outStr, outStr.contains("No active transactions found"));

        assertFalse(outStr, outStr.contains(String.valueOf("tx-label-")));

        try (Transaction tx = startTx(client1, OPTIMISTIC, SERIALIZABLE, "tx-label-3");
             Transaction tx2 = startTx(client2, OPTIMISTIC, SERIALIZABLE, "tx-label-4")) {
            checkExecute("--tx", "dur", "0");

            assertFalse(outStr, outStr.contains("No active transactions found"));

            assertTrue(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-label-3")));

            assertTrue(outStr, outStr.contains(String.valueOf(tx2.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-label-4")));

            tx.commit();
            tx2.commit();
        }
    }

    /**
     * resetOutput output
     */
    private void resetOutput() {
        out.reset();
        err.reset();
        outStr = null;
    }

    /**
     */
    private Transaction startTx(IgniteEx grid, TransactionConcurrency concurrency, TransactionIsolation isolation,
        String lb) {
        Transaction transaction = grid.transactions().withLabel(lb).txStart(concurrency, isolation);

        IgniteCache<Integer, Integer> cache = grid.cache(CACHE1);

        cache.put(val, val++);
        cache.put(val, val++);
        cache.put(val, val++);

        return transaction;
    }

    /**
     * Checks transaction with specified concurrency and isolation.
     */
    private void checkTxWithDuration(TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Exception {

        IgniteEx client1 = grid(1);
        IgniteEx client2 = grid(2);

        assertTrue(client1.configuration().isClientMode());

        try (Transaction tx = startTx(client1, concurrency, isolation, "tx-label-1")) {

            checkExecute("--tx", "dur", "3");

            assertTrue(outStr, outStr.contains("No active transactions found"));

            assertFalse(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertFalse(outStr, outStr.contains(String.valueOf("tx-label-1")));

            Thread.sleep(5_000);

            IgniteUuid tx2Xid;

            try (Transaction tx2 = startTx(client2, concurrency, isolation, "tx-label-2")) {
                tx2Xid = tx2.xid();

                checkExecute("--tx", "dur", "5");

                assertFalse(outStr, outStr.contains("No active transactions found"));

                assertTrue(outStr, outStr.contains(String.valueOf(tx.xid())));
                assertTrue(outStr, outStr.contains(String.valueOf("tx-label-1")));

                assertFalse(outStr, outStr.contains(String.valueOf(tx2Xid)));
                assertFalse(outStr, outStr.contains(String.valueOf("tx-label-2")));

                Thread.sleep(5000);

                checkExecute("--tx", "dur", "5");

                assertTrue(outStr, outStr.contains(String.valueOf(tx.xid())));
                assertTrue(outStr, outStr.contains(String.valueOf("tx-label-1")));

                assertTrue(outStr, outStr.contains(String.valueOf(tx2Xid)));
                assertTrue(outStr, outStr.contains(String.valueOf("tx-label-2")));

                tx2.commit();
            }

            checkExecute("--tx", "dur", "10");

            assertTrue(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-label-1")));

            assertFalse(outStr, outStr.contains(String.valueOf(tx2Xid)));
            assertFalse(outStr, outStr.contains(String.valueOf("tx-label-2")));

            tx.commit();
        }

        checkExecute("--tx", "dur", "0");

        assertTrue(outStr, outStr.contains("No active transactions found"));

        assertFalse(outStr, outStr.contains(String.valueOf("tx-label-")));
    }

    /**
     * test for transaction duration argument
     */
    public void testTxWithDuration() throws Exception {
        client = false;

        startGrids(1);

        client = true;

        startGridsMultiThreaded(1, 2);

        client = false;

        for (TransactionConcurrency c : TransactionConcurrency.values())
            for (TransactionIsolation iso : TransactionIsolation.values())
                checkTxWithDuration(c, iso);
    }

    /**
     * test for transaction label argument
     */
    public void testTxWithLabel() throws Exception {
        client = false;

        startGrids(1);

        client = true;

        startGridsMultiThreaded(1, 2);

        client = false;

        try {
            IgniteEx client1 = grid(1);
            IgniteEx client2 = grid(2);

            assertTrue(client1.configuration().isClientMode());

            TransactionConcurrency concurrency = TransactionConcurrency.PESSIMISTIC;

            TransactionIsolation isolation = TransactionIsolation.READ_COMMITTED;

            try (Transaction tx = startTx(client1, concurrency, isolation, "test-tx-label-1")) {

                checkExecute("--tx", "label", "test-tx-label-1");

                assertFalse(outStr, outStr.contains("No active transactions found"));

                assertTrue(outStr, outStr.contains(String.valueOf(tx.xid())));
                assertTrue(outStr, outStr.contains(String.valueOf("test-tx-label-1")));

                try (Transaction tx2 = startTx(client2, concurrency, isolation, "Test-Tx-Label-2")) {

                    checkExecute("--tx", "label", "test-tx-label-2");

                    assertFalse(outStr, outStr.contains("No active transactions found"));

                    assertFalse(outStr, outStr.contains(String.valueOf(tx.xid())));
                    assertFalse(outStr, outStr.contains(String.valueOf("test-tx-label-1")));

                    assertTrue(outStr, outStr.contains(String.valueOf(tx2.xid())));
                    assertTrue(outStr, outStr.contains(String.valueOf("Test-Tx-Label-2")));

                    checkExecute("--tx", "label", "test-tx-[\\w-]+");

                    assertFalse(outStr, outStr.contains("No active transactions found"));

                    assertTrue(outStr, outStr.contains(String.valueOf(tx.xid())));
                    assertTrue(outStr, outStr.contains(String.valueOf("test-tx-label-1")));

                    assertTrue(outStr, outStr.contains(String.valueOf(tx2.xid())));
                    assertTrue(outStr, outStr.contains(String.valueOf("Test-Tx-Label-2")));
                }
            }
        }
        finally {
            outStr = err.toString();

            if (!F.isEmpty(outStr))
                log().error(outStr);
        }
    }

    /**
     * test for clients argument
     */
    public void testTxWithClients() throws Exception {
        client = false;

        startGrids(1);

        client = true;

        startGridsMultiThreaded(1, 1);

        client = false;

        IgniteEx srv = grid(0);
        IgniteEx client = grid(1);

        assertTrue(client.configuration().isClientMode());

        TransactionConcurrency concurrency = TransactionConcurrency.PESSIMISTIC;

        TransactionIsolation isolation = TransactionIsolation.READ_COMMITTED;

        try (Transaction tx = startTx(srv, concurrency, isolation, "tx-label-1")) {

            checkExecute("--tx");

            assertFalse(outStr, outStr.contains("No active transactions found"));

            assertTrue(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-label-1")));

            checkExecute("--tx", "clients");

            assertTrue(outStr, outStr.contains("No active transactions found"));

            assertFalse(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertFalse(outStr, outStr.contains(String.valueOf("tx-label-1")));
            try (Transaction tx2 = startTx(client, concurrency, isolation, "tx-client-2")) {
                checkExecute("--tx", "clients");

                assertFalse(outStr, outStr.contains("No active transactions found"));

                assertFalse(outStr, outStr.contains(String.valueOf(tx.xid())));
                assertFalse(outStr, outStr.contains(String.valueOf("tx-label-1")));

                assertTrue(outStr, outStr.contains(String.valueOf(tx2.xid())));
                assertTrue(outStr, outStr.contains(String.valueOf("tx-client-2")));
            }
        }
    }

    /**
     * test for nodes argument
     */
    public void testTxWithNodes() throws Exception {
        client = false;

        startGrids(1);

        client = true;

        startGridsMultiThreaded(1, 2);

        client = false;

        IgniteEx srv = grid(0);
        String srvId = srv.localNode().consistentId().toString();

        IgniteEx client = grid(1);
        String client1Id = client.localNode().consistentId().toString();

        client = grid(2);
        String clientId = client.localNode().consistentId().toString();

        assertTrue(client.configuration().isClientMode());

        TransactionConcurrency concurrency = OPTIMISTIC;

        TransactionIsolation isolation = SERIALIZABLE;

        try (Transaction tx = startTx(srv, concurrency, isolation, "tx-srv-1");
             Transaction tx2 = startTx(client, concurrency, isolation, "tx-client-2")) {

            checkExecute("--tx", "nodes", srvId);

            assertFalse(outStr, outStr.contains("No active transactions found"));

            assertTrue(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-srv-1")));

            assertFalse(outStr, outStr.contains(String.valueOf(tx2.xid())));
            assertFalse(outStr, outStr.contains(String.valueOf("tx-client-2")));

            checkExecute("--tx", "nodes", clientId);

            assertFalse(outStr, outStr.contains("No active transactions found"));

            assertFalse(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertFalse(outStr, outStr.contains(String.valueOf("tx-srv-1")));

            assertTrue(outStr, outStr.contains(String.valueOf(tx2.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-client-2")));

            checkExecute("--tx", "nodes", clientId + "," + srvId);

            assertFalse(outStr, outStr.contains("No active transactions found"));

            assertTrue(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-srv-1")));

            assertTrue(outStr, outStr.contains(String.valueOf(tx2.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-client-2")));

            checkExecute("--tx", "nodes", srvId, "clients");

            assertFalse(outStr, outStr.contains("No active transactions found"));

            assertTrue(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-srv-1")));

            assertTrue(outStr, outStr.contains(String.valueOf(tx2.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-client-2")));

            checkExecute("--tx", "nodes", client1Id);

            assertTrue(outStr, outStr.contains("No active transactions found"));

            assertFalse(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertFalse(outStr, outStr.contains(String.valueOf("tx-srv-1")));

            assertFalse(outStr, outStr.contains(String.valueOf(tx2.xid())));
            assertFalse(outStr, outStr.contains(String.valueOf("tx-client-2")));

            assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--tx", "nodes", "dummyId"));

            assertTrue(outStr, outStr.contains("Task map operation produced no mapped jobs"));
        }
    }

    public void testTxStop() throws Exception {
        client = false;

        startGrids(1);

        client = true;

        startGridsMultiThreaded(1, 2);

        client = false;

        IgniteEx srv = grid(0);
        String srvId = srv.localNode().consistentId().toString();

        IgniteEx client = grid(1);
        String client1Id = client.localNode().consistentId().toString();

        client = grid(2);
        String clientId = client.localNode().consistentId().toString();

        assertTrue(client.configuration().isClientMode());

        try (Transaction tx = startTx(srv, OPTIMISTIC, SERIALIZABLE, "tx-srv-1");
             Transaction tx2 = startTx(client, PESSIMISTIC, READ_COMMITTED, "tx-client-2")) {

            checkExecute("--tx");

            assertFalse(outStr, outStr.contains("No active transactions found"));

            assertTrue(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-srv-1")));

            assertTrue(outStr, outStr.contains(String.valueOf(tx2.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-client-2")));

            log.info(outStr);

            checkExecute("--tx", "stop", tx.xid().toString(), "nodes", client1Id);

            assertTrue(outStr, outStr.contains("No active transactions found"));

            log.info(outStr);

            checkExecute("--tx", "stop", tx.xid().toString());

            assertTrue(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-srv-1")));

            log.info(outStr);

            checkExecute("--tx");

            assertFalse(outStr, outStr.contains("No active transactions found"));

            assertFalse(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertFalse(outStr, outStr.contains(String.valueOf("tx-srv-1")));

            assertTrue(outStr, outStr.contains(String.valueOf(tx2.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-client-2")));

            log.info(outStr);

            checkExecute("--tx", "stop", tx2.xid().toString());

            assertTrue(outStr, outStr.contains(String.valueOf(tx2.xid())));
            assertTrue(outStr, outStr.contains(String.valueOf("tx-client-2")));

            log.info(outStr);

            checkExecute("--tx");

            assertTrue(outStr, outStr.contains("No active transactions found"));

            assertFalse(outStr, outStr.contains(String.valueOf(tx.xid())));
            assertFalse(outStr, outStr.contains(String.valueOf("tx-srv-1")));

            assertFalse(outStr, outStr.contains(String.valueOf(tx2.xid())));
            assertFalse(outStr, outStr.contains(String.valueOf("tx-client-2")));

            log.info(outStr);
        }
    }
}

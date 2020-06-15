package org.apache.ignite.internal.processors.cache.distributed;

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Assert;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

// TODO extends IgniteTxTimeoutAbstractTest
public class Sdsb11817Test extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_COUNT = 3;

    /** */
    private static final int NODE_0_INDEX = 0;

    /** */
    private static final int NODE_1_INDEX = 1;

    /** */
    private static final int NODE_2_INDEX = 2;

    /** Transaction timeout. */
    private static final long TIMEOUT = 5000;

    /** Keys count. */
    private static final int KEY_COUNT_PER_NODE = 10;

    /** Logger */
    private final ListeningTestLogger testLogger = new ListeningTestLogger(true, null);

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        testLogger.clearListeners();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        testLogger.clearListeners();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        TransactionConfiguration txCfg = c.getTransactionConfiguration();

        txCfg.setDefaultTxTimeout(TIMEOUT);
        txCfg.setPessimisticTxLogLinger((int)(TIMEOUT/2));

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        c.setDiscoverySpi(spi);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(1);
        cc.setAtomicityMode(TRANSACTIONAL);

        //cacheCfg.setPreloadMode(NONE);

        c.setCacheConfiguration(cc);

        c.setGridLogger(testLogger);

        return c;
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticReadCommitted() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticRepeatableRead() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticSerializable() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticReadCommitted() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticRepeatableRead() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticSerializable() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * Scenario:
     * <ol>
     *     <li>Start 3 node</li>
     *     <li>Create transaction on node 0</li>
     *     <li>Put key (key affinity for node 1)</li>
     *     <li>Put key (key affinity for node 2)</li>
     *     <li>Commit</li>
     *     <li>After prepared transaction emulate network problem</li>
     *     <li>Find in log long running transaction where duration > timeout</li>
     * </ol>
     *
     * @param concurrency Concurrency.
     * @param isolation   Isolation.
     * @throws IgniteCheckedException If test failed.
     */
    private void checkTransactionTimeout(TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Exception {
        IgniteEx igniteEx = startGrids(GRID_COUNT);
        igniteEx.cluster().active(true);

        IgniteCache<Integer, String> cacheNode0 = jcache(NODE_0_INDEX);
        IgniteCache<Integer, String> cacheNode1 = jcache(NODE_1_INDEX);
        IgniteCache<Integer, String> cacheNode2 = jcache(NODE_2_INDEX);

        final List<Integer> keyNode1 = primaryKeys(cacheNode1, KEY_COUNT_PER_NODE);
        final List<Integer> keyNode2 = primaryKeys(cacheNode2, KEY_COUNT_PER_NODE);

        // 2020-05-18 02:39:17.868|First 10 long running transactions [total=140]
        // 2020-05-18 02:39:17.868|>>> Transaction [startTime=02:37:49.251, curTime=02:39:17.680, инициатор СУ 10.104.6.29, lb=moduleId: union-module, threadName: DplThread_batch_50, isolation=READ_COMMITTED, concurrency=PESSIMISTIC, timeout=299990, state=PREPARED, duration=88429ms, onePhaseCommit=false, size=2
        testLogger.registerListener(new LogListener() {
            private boolean check=true;

            @Override public boolean check() {
                return check;
            }

            @Override public void reset() {

            }

            @Override public void accept(String s) {
                int index = s.indexOf(">>> Transaction [");
                if (index>=0){
                    index+=17;
                    index=s.indexOf("timeout=",index);
                    if (index>0){
                        index+=8;
                        int indexEnd=s.indexOf(',',index);
                        if (indexEnd>0) {
                            long timeout =Long.parseLong(s.substring(index, indexEnd));

                            index=s.indexOf("duration=",index);
                            if (index>0){
                                index+=9;
                                indexEnd=s.indexOf(',',index);
                                if (indexEnd>0) {
                                    long duration =Long.parseLong(s.substring(index, indexEnd));
                                    Assert.assertTrue("Find transaction where duration>timeout ("+duration+","+timeout+")", duration<=timeout);
                                }
                            }
                        }
                    }
                }
            }
        });

        try (Transaction tx = ignite(NODE_0_INDEX).transactions().txStart(concurrency, isolation, TIMEOUT, 0)) {
            for (Integer key : keyNode1)
                cacheNode0.put(key, Integer.toString(key));

            for (Integer key : keyNode2)
                cacheNode0.put(key, Integer.toString(key));

            // TODO wait prepared and terminate/suspension connection
            tx.commit();
        }
        catch (Exception e) {
            if (!(X.hasCause(e, TransactionTimeoutException.class)))
                throw e;

        }
    }
}

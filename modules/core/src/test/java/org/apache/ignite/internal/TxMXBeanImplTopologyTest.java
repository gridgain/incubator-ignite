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

package org.apache.ignite.internal;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.mxbean.TxMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class TxMXBeanImplTopologyTest extends GridCommonAbstractTest {

    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);
    public static final int TRANSACTIONS = 10;
    public static final int STARTUP_TIMEOUT_MS = 1000; // fixme

    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        final CacheConfiguration cCfg = new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setAtomicityMode(TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        super.afterTest();
    }

    /**
     *
     */
    public void testNearOnBackup() throws Exception {
        IgniteEx primaryNode = startGrid(0);
        IgniteEx backupNode = startGrid(1);
        TxMXBean txMXBeanBackup = txMXBean(1);

        awaitPartitionMapExchange();

        final IgniteCache<Integer, String> primaryCache = primaryNode.cache(DEFAULT_CACHE_NAME);

        block();

        final List<Integer> primaryKeys = primaryKeys(primaryCache, TRANSACTIONS);
        for (int key : primaryKeys)
            new Thread(new TxThread(backupNode, key)).start();

        Thread.sleep(STARTUP_TIMEOUT_MS);

        final Map<String, String> transactions = txMXBeanBackup.getAllLocalTransactions();
        assertEquals(TRANSACTIONS, transactions.size());

        int match = 0;
        for (String txInfo : transactions.values()) {
            if (txInfo.contains("PREPARING")
                    && txInfo.contains("NEAR")
                    && txInfo.contains("PRIMARY")
                    && !txInfo.contains("ORIGINATING"))
                match++;
        }
        assertEquals(TRANSACTIONS, match);
    }

    /**
     *
     */
    public void testRemoteOnPrimary() throws Exception {
        IgniteEx primaryNode = startGrid(0);
        IgniteEx backupNode = startGrid(1);
        TxMXBean txMxBeanPrimary = txMXBean(0);

        awaitPartitionMapExchange();

        final IgniteCache<Integer, String> primaryCache = primaryNode.cache(DEFAULT_CACHE_NAME);

        block();

        final List<Integer> primaryKeys = primaryKeys(primaryCache, TRANSACTIONS);
        for (int key : primaryKeys)
            new Thread(new TxThread(backupNode, key)).start();

        Thread.sleep(STARTUP_TIMEOUT_MS);

        final Map<String, String> transactions = txMxBeanPrimary.getAllLocalTransactions();
        assertEquals(TRANSACTIONS, transactions.size());

        int match = 0;
        for (String txInfo : transactions.values()) {
            if (txInfo.contains("PREPARING")
                && txInfo.contains("ORIGINATING")
                && !txInfo.contains("PRIMARY")
                && !txInfo.contains("NEAR"))
                match++;
        }
        assertEquals(TRANSACTIONS, match);
    }

    private void block() {
        for (Ignite ignite : G.allGrids()) {
            final TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();
            spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message message) {
//                    System.out.println("message is" + message);
                    if (message instanceof GridDhtTxPrepareRequest)
                        return true;
                    else if (message instanceof GridDhtTxPrepareResponse)
                        return true;

                    return false;
                }
            });
        }
    }

    private static class TxThread implements Runnable {
        private Ignite ignite;
        private int key;

        public TxThread(final Ignite ignite, final int key) {
            this.ignite = ignite;
            this.key = key;
        }

        @Override public void run() {
            try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                ignite.cache(DEFAULT_CACHE_NAME).put(key, Thread.currentThread().getName());
                tx.commit();
                System.out.println("done!");
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     */
    private TxMXBean txMXBean(int igniteInt) throws Exception {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(igniteInt), "Transactions", TxMXBeanImpl.class.getSimpleName());
        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();
        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());
        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, TxMXBean.class, true);
    }
}

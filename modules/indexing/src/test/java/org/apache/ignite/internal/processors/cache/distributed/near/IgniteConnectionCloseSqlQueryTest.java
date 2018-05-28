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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.lang.gridfunc.NoOpClosure;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage2;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 */
public class IgniteConnectionCloseSqlQueryTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(50 * 1024 * 1024));

        c.setDataStorageConfiguration(memCfg);

        c.setClientMode(igniteInstanceName.startsWith("client"));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();
        spi.setIdleConnectionTimeout(1000L);

        c.setCommunicationSpi(spi);

        CacheConfiguration<?, ?> cc = defaultCacheConfiguration();

        cc.setName(DEFAULT_CACHE_NAME);
        cc.setCacheMode(PARTITIONED);
        cc.setBackups(2);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setRebalanceMode(SYNC);
        cc.setAffinity(new RendezvousAffinityFunction(false, 60));
        cc.setIndexedTypes(Integer.class, Person.class);

        c.setCacheConfiguration(cc);

        return c;
    }

    private static final class TestClosure implements IgniteRunnable {
        /** Serial version uid. */
        private static final long serialVersionUid = 0L;

        @LoggerResource
        private IgniteLogger log;

        @Override public void run() {
            log.info("INVOKE");
        }
    }


    public void test() throws Exception {
        IgniteEx svr = startGrid(0);

        Ignite c1 = startGrid("client1");

        assertTrue(c1.configuration().isClientMode());

        Ignite c2 = startGrid("client2");

        assertTrue(c2.configuration().isClientMode());

        TestRecordingCommunicationSpi spi2 = (TestRecordingCommunicationSpi)c1.configuration().getCommunicationSpi();

        spi2.blockMessages(HandshakeMessage2.class, c1.name());

        AtomicBoolean stop = new AtomicBoolean();

        c1.compute(c1.cluster().forNodeId(c2.cluster().localNode().id())).run(new TestClosure());

        TcpCommunicationSpi spi1 = (TcpCommunicationSpi)c1.configuration().getCommunicationSpi();

        ConcurrentMap<UUID, GridCommunicationClient[]> clientsMap = U.field(spi1, "clients");

        GridCommunicationClient[] arr = clientsMap.get(c2.cluster().localNode().id());

        GridTcpNioCommunicationClient client = null;

        for (GridCommunicationClient c : arr) {
            client = (GridTcpNioCommunicationClient)c;

            if(client != null) {
                assertTrue(client.session().outRecoveryDescriptor().reserved());

                assertFalse(client.session().outRecoveryDescriptor().connected());
            }
        }

        doSleep(3000);

        assertNotNull(client);

        client.session().outRecoveryDescriptor().printDebugInfo(new StringBuilder());

        c1.compute(c1.cluster().forNodeId(c2.cluster().localNode().id())).run(new TestClosure());
//
//        System.out.println();

//        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
//            @Override public void run() {
//                while(!stop.get())
//                    c1.compute(c1.cluster().forNodeId(c2.cluster().localNode().id())).run(new NoOpClosure());
//            }
//        }, 8, "send-thread");
//
//        doSleep(15_000);

        stop.set(true);

        //fut.get();
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        @QuerySqlField(index = true)
        int id;

        /**
         * @param id Person ID.
         */
        Person(int id) {
            this.id = id;
        }
    }
}

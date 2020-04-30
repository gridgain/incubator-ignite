/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateInvokeRequest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL;

public class AssertionErrorDeploymentTest extends GridCommonAbstractTest {
    /** Listening logger. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(true, log);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        listeningLog.clearListeners();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setClientMode(igniteInstanceName.contains("client"))
            .setPeerClassLoadingEnabled(true)
            .setFailureHandler(new StopNodeFailureHandler())
            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                    .setBackups(1)
            );
    }

    @Test
    public void test() throws Exception {
        withSystemProperty(IGNITE_CACHE_REMOVED_ENTRIES_TTL, "1000000");

        IgniteEx crd = startGrids(1);
        Ignite client = startGrid("client");

        awaitPartitionMapExchange();

        LogListener logLsnr = LogListener.matches("Removed undeployed class").build();

        LogListener logLsnr0 = LogListener.matches(logStr -> {
            if (logStr.startsWith("Retrieved auto-loaded resource from spi:") &&
                logStr.contains(TestCacheEntryProcessor.class.getSimpleName())) {

                System.out.println("dirty getting a breakdown location " + logStr);
                crd.compute().localDeployTask(TestTaskVer2.class, new TestClassLoader());
                return true;
            }

            return false;
        }).build();

        listeningLog.registerListener(logLsnr);
        listeningLog.registerListener(logLsnr0);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(client);

        spi.blockMessages((node, msg) -> {
            log.info("SEE msg=" + msg.getClass().getSimpleName() + " " + msg);
            return GridNearAtomicSingleUpdateInvokeRequest.class.isInstance(msg);
        });

        CountDownLatch latch = new CountDownLatch(1);

        GridTestUtils.runAsync(() -> {
            latch.countDown();
            spi.waitForBlocked();

            try {
                crd.compute().localDeployTask(TestTask.class, new TestClassLoader());
            }
            finally {
                spi.stopBlock();
            }

            return null;
        });

        latch.await();

        client.cache(DEFAULT_CACHE_NAME).invoke(1, new TestCacheEntryProcessor());

        doSleep(1_000);

        assertTrue(logLsnr.check());
        assertTrue(logLsnr0.check());
    }

    /**
     * Test {@link CacheEntryProcessor}.
     */
    private static class TestCacheEntryProcessor implements CacheEntryProcessor<Object, Object, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> entry, Object... objects) throws EntryProcessorException {
            return 2;
        }
    }

    /**
     * Special extension of {@link TestTask}.
     */
    @ComputeTaskName("org.apache.ignite.internal.AssertionErrorDeploymentTest$TestCacheEntryProcessor")
    private static class TestTaskVer2 extends TestTask {
        //no-op
    }

    /**
     * Test {@link ComputeTaskAdapter}.
     */
    private static class TestTask extends ComputeTaskAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) {
            assert false;

            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return new Object();
        }
    }

    /**
     * Test {@link ClassLoader}.
     */
    private static class TestClassLoader extends ClassLoader {
        /** {@inheritDoc} */
        @Override protected Class<?> findClass(String name) throws ClassNotFoundException {
            return Thread.currentThread().getContextClassLoader().loadClass(name);
        }
    }
}

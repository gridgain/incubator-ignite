package org.apache.ignite.internal.processors.cache.datastructures;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateAbstractTest.TEST_TIMEOUT;

public class GridCacheAtomicLongNseTest  extends IgniteCollectionAbstractTest {

    private static final String STRUCTURE_NAME = "myAtomicLong";

    private static final int START_GRIDS = 15;

    private static final int CLIENTS_COUNT = 15;

    private static final int ADDITIONAL_GRIDS = 3;

    private static final int INTERVAL_SLEEP_MS = 10;

    protected IgniteConfiguration getClientConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(getTestIgniteInstanceName(idx));
        cfg.setClientMode(true);
        return cfg;
    }

    @Test
    public void doTest() throws Exception {

        for(int i=START_GRIDS; i < START_GRIDS + CLIENTS_COUNT; i++){
            startGrid(getClientConfiguration(i));
        }

        CountDownLatch latch = new CountDownLatch(gridCount());

        for (int i = START_GRIDS; i < START_GRIDS + CLIENTS_COUNT; i++) {

            int finalI = i;
            Thread t = new Thread(() -> {

                IgniteAtomicLong ial = null;
                IgniteEx grid = grid(finalI);
                long cnt = 0;
                while (ial == grid.atomicLong("dataNodeLoaded", 0, false)) {
                    System.out.println("ial while loop " + Thread.currentThread().getId());
                    cnt++;

                    try {
                        Thread.sleep(INTERVAL_SLEEP_MS);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if(cnt % finalI == 0 && finalI > 3 && cnt > finalI * 100){
                        stopGrid(finalI);
                        latch.countDown();
                        break;
                    }

                }

                System.out.println("ial while loop ended");

                latch.countDown();

            });

            t.start();
        }


        Thread.sleep(100);

        int startIdx = START_GRIDS + CLIENTS_COUNT + 1;

        for(int i=startIdx; i < startIdx + ADDITIONAL_GRIDS; i++){
            startGrid(getClientConfiguration(i));
        }

        System.out.println("New Grid started");

        for (int i = startIdx; i < startIdx + ADDITIONAL_GRIDS; i++) {

            int finalI = i;
            Thread t = new Thread(() -> {

                IgniteAtomicLong ial = null;
                while (ial == grid(finalI).atomicLong("dataNodeLoaded", 1, true)) {
                    System.out.println("ial while loop2");

                    try {
                        Thread.sleep(100);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }

                System.out.println("ial while loop ended2");
            });

            t.start();
        }

        System.out.println("Waiting");


        if (!latch.await(3000, TimeUnit.MILLISECONDS))
            fail("Something went wrong.");

    }


    @Override
    protected int gridCount() {
        return START_GRIDS;
    }

    @Override
    protected CacheMode collectionCacheMode() {
        return CacheMode.REPLICATED;
    }

    @Override
    protected CacheAtomicityMode collectionCacheAtomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }
}

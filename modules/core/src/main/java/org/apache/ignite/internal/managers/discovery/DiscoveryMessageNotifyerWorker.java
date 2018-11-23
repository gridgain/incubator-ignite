package org.apache.ignite.internal.managers.discovery;

import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.spi.discovery.IgniteDiscoveryThread;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;

/**
 *
 */
public class DiscoveryMessageNotifyerWorker extends GridWorker implements IgniteDiscoveryThread {
    private GridDiscoveryManager gridDiscoveryManager;
    /** Queue. */
    private final BlockingQueue<T2<GridFutureAdapter, Runnable>> queue = new LinkedBlockingQueue<>();

    private volatile long lastTake;

    /**
     * Default constructor.
     */
    protected DiscoveryMessageNotifyerWorker(GridDiscoveryManager gridDiscoveryManager) {
        super(gridDiscoveryManager.ctx.igniteInstanceName(), "disco-notyfier-worker", gridDiscoveryManager.log, gridDiscoveryManager.ctx.workersRegistry());
        this.gridDiscoveryManager = gridDiscoveryManager;
    }

    /**
     *
     */
    private void body0() throws InterruptedException {
        T2<GridFutureAdapter, Runnable> notification = queue.take();

        lastTake = System.currentTimeMillis();

        try {
            notification.get2().run();
        }
        finally {
            boolean done = notification.get1().onDone();

            log.info("DiscoveryMessageNotifyerWorker.body0 Done " + done);
        }
    }

    /**
     * @param cmd Command.
     */
    public synchronized void submit(GridFutureAdapter notificationFut, Runnable cmd) {
        if (isCancelled()) {
            notificationFut.onDone();

            return;
        }

        log.info("Time since last take : " + (System.currentTimeMillis() - lastTake)
                + " queue.size() " + queue.size());

        queue.add(new T2<>(notificationFut, cmd));
    }

    /**
     * Cancel thread execution and completes all notification futures.
     */
    @Override public synchronized void cancel() {
        log.info("cancel");

        super.cancel();

        log.info("queue.size() = " + queue.size());

        while (!queue.isEmpty()) {
            T2<GridFutureAdapter, Runnable> notification = queue.poll();

            if (notification != null)
                notification.get1().onDone();
        }
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        while (!isCancelled()) {
            try {
                body0();
            }
            catch (InterruptedException e) {
                if (!isCancelled)
                    gridDiscoveryManager.ctx.failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, e));

                throw e;
            }
            catch (Throwable t) {
                U.error(log, "Exception in discovery notyfier worker thread.", t);

                FailureType type = t instanceof OutOfMemoryError ? CRITICAL_ERROR : SYSTEM_WORKER_TERMINATION;

                gridDiscoveryManager.ctx.failure().process(new FailureContext(type, t));

                throw t;
            }
        }
    }
}

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerListener;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Superclass for all message workers.
 *
 * @param <T> Message type.
 */
public abstract class MessageWorker<T> extends GridWorker {
    /** Message queue. */
    protected final BlockingDeque<T> queue = new LinkedBlockingDeque<>();

    /** Polling timeout. */
    private final long pollingTimeout;

    private final TcpDiscoverySpi spi;

    /**
     * @param name Worker name.
     * @param log Logger.
     * @param pollingTimeout Messages polling timeout.
     * @param lsnr Listener for life-cycle events.
     */
    MessageWorker(
            String name,
            IgniteLogger log,
            long pollingTimeout,
            @Nullable GridWorkerListener lsnr,
            TcpDiscoverySpi spi

    ) {
        super(spi.ignite().name(), name, log, lsnr);

        this.pollingTimeout = pollingTimeout;

        this.spi = spi;
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException {
        if (log.isDebugEnabled())
            log.debug("Message worker started [locNodeId=" + spi.cfgNodeId + ']');

        while (!isCancelled()) {
            T msg = queue.poll(pollingTimeout, TimeUnit.MILLISECONDS);

            if (msg == null)
                noMessageLoop();
            else
                processMessage(msg);
        }
    }

    /**
     * @return Current message queue size.
     */
    int queueSize() {
        return queue.size();
    }

    /**
     * Processes succeeding message.
     *
     * @param msg Message.
     */
    protected abstract void processMessage(T msg);

    /**
     * Called when there is no message to process giving ability to perform other activity.
     */
    protected void noMessageLoop() {
        // No-op.
    }

    /**
     * Actions to be done before worker termination.
     */
    protected void tearDown() {
        // No-op.
    }
}

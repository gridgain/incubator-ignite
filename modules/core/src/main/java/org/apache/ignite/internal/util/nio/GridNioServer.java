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

package org.apache.ignite.internal.util.nio;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import org.apache.ignite.internal.processors.tracing.NoopSpan;
import org.apache.ignite.internal.processors.tracing.NoopTracing;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanTags;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.nio.filter.GridAbstractNioFilter;
import org.apache.ignite.internal.util.nio.filter.GridNioFilter;
import org.apache.ignite.internal.util.nio.filter.GridNioFilterChain;
import org.apache.ignite.internal.util.nio.operation.DumpStatsFuture;
import org.apache.ignite.internal.util.nio.operation.NioOperation;
import org.apache.ignite.internal.util.nio.operation.RecalculateIdleTimeoutRequest;
import org.apache.ignite.internal.util.nio.operation.SessionCloseFuture;
import org.apache.ignite.internal.util.nio.operation.SessionFinishMoveRequest;
import org.apache.ignite.internal.util.nio.operation.SessionMoveRequest;
import org.apache.ignite.internal.util.nio.operation.SessionOperation;
import org.apache.ignite.internal.util.nio.operation.SessionOperationFuture;
import org.apache.ignite.internal.util.nio.operation.SessionOperationRequest;
import org.apache.ignite.internal.util.nio.operation.SessionRegisterFuture;
import org.apache.ignite.internal.util.nio.operation.SessionStartMoveRequest;
import org.apache.ignite.internal.util.nio.operation.SessionWriteFuture;
import org.apache.ignite.internal.util.nio.operation.SessionWriteRequest;
import org.apache.ignite.internal.util.nio.operation.SessionWriteRequestImpl;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerListener;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.processors.tracing.SpanType.COMMUNICATION_SOCKET_WRITE;
import static org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesTable.traceName;
import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.MSG_WRITER;
import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.NIO_OPERATION;

/**
 * TCP NIO server. Due to asynchronous nature of connections processing
 * network events such as client connection, disconnection and message receiving are passed to
 * the server listener. Once client connected, an associated {@link GridNioSession} object is
 * created and can be used in communication.
 * <p>
 * This implementation supports several selectors and several reading threads.
 *
 * @param <T> Message type.
 *
 */
public class GridNioServer<T> {
    /** */
    public static final String IGNITE_IO_BALANCE_RANDOM_BALANCE = "IGNITE_IO_BALANCE_RANDOM_BALANCER";

    /** Default session write timeout. */
    public static final int DFLT_SES_WRITE_TIMEOUT = 5000;

    /** Default send queue limit. */
    public static final int DFLT_SEND_QUEUE_LIMIT = 0;

    /** Time, which server will wait before retry operation. */
    private static final long ERR_WAIT_TIME = 2000;

    /** Buffer metadata key. */
    private static final int BUF_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** SSL system data buffer metadata key. */
    private static final int BUF_SSL_SYSTEM_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** SSL write buf limit. */
    private static final int WRITE_BUF_LIMIT = GridNioSessionMetaKey.nextUniqueKey();

    /** Session future meta key. */
    public static final int RECOVERY_DESC_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Selection key meta key. */
    private static final int WORKER_IDX_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Meta key for pending requests to be written. */
    private static final int REQUESTS_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    private static final boolean DISABLE_KEYSET_OPTIMIZATION =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_NO_SELECTOR_OPTS);

    /** */
    public static final String OUTBOUND_MESSAGES_QUEUE_SIZE_METRIC_NAME = "outboundMessagesQueueSize";

    /** */
    public static final String OUTBOUND_MESSAGES_QUEUE_SIZE_METRIC_DESC = "Number of messages waiting to be sent";

    /** */
    public static final String RECEIVED_BYTES_METRIC_NAME = "receivedBytes";

    /** */
    public static final String RECEIVED_BYTES_METRIC_DESC = "Total number of bytes received by current node";

    /** */
    public static final String SENT_BYTES_METRIC_NAME = "sentBytes";

    /** */
    public static final String SENT_BYTES_METRIC_DESC = "Total number of bytes sent by current node";

    /** Defines how many times selector should do {@code selectNow()} before doing {@code select(long)}. */
    private final long selectorSpins;

    /** Accept worker. */
    @GridToStringExclude
    private final GridNioAcceptWorker acceptWorker;

    /** Read worker threads. */
    private final IgniteThread[] clientThreads;

    /** Read workers. */
    private final List<AbstractNioClientWorker> clientWorkers;

    /** Filter chain to use. */
    private final GridNioFilterChain<T> filterChain;

    /** Server listener. */
    private final GridNioServerListener<T> lsnr;

    /** Logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /** Closed flag. */
    private volatile boolean closed;

    /** Flag indicating if this server should use direct buffers. */
    private final boolean directBuf;

    /** Index to select which thread will serve next incoming socket channel. Using round-robin balancing. */
    @GridToStringExclude
    private int readBalanceIdx;

    /** Index to select which thread will serve next out socket channel. Using round-robin balancing. */
    @GridToStringExclude
    private int writeBalanceIdx = 1;

    /** Tcp no delay flag. */
    private final boolean tcpNoDelay;

    /** Socket send buffer. */
    private final int sockSndBuf;

    /** Socket receive buffer. */
    private final int sockRcvBuf;

    /** Write timeout */
    private volatile long writeTimeout = DFLT_SES_WRITE_TIMEOUT;

    /** Idle timeout. */
    private volatile long idleTimeout = ConnectorConfiguration.DFLT_IDLE_TIMEOUT;

    /** For test purposes only. */
    private boolean skipWrite;

    /** For test purposes only. */
    private boolean skipRead;

    /** Local address. */
    private final InetSocketAddress locAddr;

    /** Order. */
    private final ByteOrder order;

    /** Send queue limit. */
    private final int sndQueueLimit;

    /** Whether direct mode is used. */
    private final boolean directMode;

    /** */
    @Nullable private final MetricRegistry mreg;

    /** Received bytes count metric. */
    @Nullable private final LongAdderMetric rcvdBytesCntMetric;

    /** Sent bytes count metric. */
    @Nullable private final LongAdderMetric sentBytesCntMetric;

    /** Outbound messages queue size. */
    @Nullable private final LongAdderMetric outboundMessagesQueueSizeMetric;

    /** Sessions. */
    private final GridConcurrentHashSet<GridSelectorNioSessionImpl> sessions = new GridConcurrentHashSet<>();

    /** */
    private GridNioSslFilter sslFilter;

    /** */
    @GridToStringExclude
    private GridNioMessageWriterFactory writerFactory;

    /** */
    @GridToStringExclude
    private IgnitePredicate<Message> skipRecoveryPred;

    /** Optional listener to monitor outbound message queue size. */
    private IgniteBiInClosure<GridNioSession, Integer> msgQueueLsnr;

    /** */
    private final AtomicLong readerMoveCnt = new AtomicLong();

    /** */
    private final AtomicLong writerMoveCnt = new AtomicLong();

    /** */
    private final IgniteRunnable balancer;

    /**
     * Interval in milliseconds between consequtive {@link GridWorkerListener#onIdle(GridWorker)} calls
     * in server workers.
     */
    private final boolean readWriteSelectorsAssign;

    /** Tracing processor. */
    private Tracing tracing;

    /**
     * @param addr Address.
     * @param port Port.
     * @param log Log.
     * @param selectorCnt Count of selectors and selecting threads.
     * @param igniteInstanceName Ignite instance name.
     * @param srvName Logical server name for threads identification.
     * @param selectorSpins Defines how many non-blocking {@code selector.selectNow()} should be made before
     *      falling into {@code selector.select(long)} in NIO server. Long value. Default is {@code 0}.
     *      Can be set to {@code Long.MAX_VALUE} so selector threads will never block.
     * @param tcpNoDelay If TCP_NODELAY option should be set to accepted sockets.
     * @param directBuf Direct buffer flag.
     * @param order Byte order.
     * @param lsnr Listener.
     * @param sockSndBuf Socket send buffer.
     * @param sockRcvBuf Socket receive buffer.
     * @param sndQueueLimit Send queue limit.
     * @param directMode Whether direct mode is used.
     * @param daemon Daemon flag to create threads.
     * @param writerFactory Writer factory.
     * @param skipRecoveryPred Skip recovery predicate.
     * @param msgQueueLsnr Message queue size listener.
     * @param readWriteSelectorsAssign If {@code true} then in/out connections are assigned to even/odd workers.
     * @param workerLsnr Worker lifecycle listener.
     * @param mreg Metrics registry.
     * @param filters Filters for this server.
     * @throws IgniteCheckedException If failed.
     */
    private GridNioServer(
        InetAddress addr,
        int port,
        IgniteLogger log,
        int selectorCnt,
        @Nullable String igniteInstanceName,
        @Nullable String srvName,
        long selectorSpins,
        boolean tcpNoDelay,
        boolean directBuf,
        ByteOrder order,
        GridNioServerListener<T> lsnr,
        int sockSndBuf,
        int sockRcvBuf,
        int sndQueueLimit,
        boolean directMode,
        boolean daemon,
        GridNioMessageWriterFactory writerFactory,
        IgnitePredicate<Message> skipRecoveryPred,
        IgniteBiInClosure<GridNioSession, Integer> msgQueueLsnr,
        boolean readWriteSelectorsAssign,
        @Nullable GridWorkerListener workerLsnr,
        @Nullable MetricRegistry mreg,
        Tracing tracing,
        GridNioFilter... filters
    ) throws IgniteCheckedException {
        if (port != -1)
            A.notNull(addr, "addr");

        A.notNull(lsnr, "lsnr");
        A.notNull(log, "log");
        A.notNull(order, "order");

        A.ensure(port == -1 || (port > 0 && port < 0xffff), "port");
        A.ensure(selectorCnt > 0, "selectorCnt");
        A.ensure(sockRcvBuf >= 0, "sockRcvBuf");
        A.ensure(sockSndBuf >= 0, "sockSndBuf");
        A.ensure(sndQueueLimit >= 0, "sndQueueLimit");

        this.log = log;
        this.directBuf = directBuf;
        this.order = order;
        this.tcpNoDelay = tcpNoDelay;
        this.sockRcvBuf = sockRcvBuf;
        this.sockSndBuf = sockSndBuf;
        this.sndQueueLimit = sndQueueLimit;
        this.msgQueueLsnr = msgQueueLsnr;
        this.selectorSpins = selectorSpins;
        this.readWriteSelectorsAssign = readWriteSelectorsAssign;
        this.lsnr = lsnr;
        this.tracing = tracing == null ? new NoopTracing() : tracing;

        filterChain = new GridNioFilterChain<>(log, lsnr, new HeadFilter(), filters);

        if (directMode) {
            for (GridNioFilter filter : filters) {
                if (filter instanceof GridNioSslFilter) {
                    sslFilter = (GridNioSslFilter)filter;

                    assert sslFilter.directMode();
                }
            }
        }

        if (port != -1) {
            // Once bind, we will not change the port in future.
            locAddr = new InetSocketAddress(addr, port);

            // This method will throw exception if address already in use.
            Selector acceptSelector = createSelector(locAddr);

            String threadName;

            if (srvName == null)
                threadName = "nio-acceptor";
            else
                threadName = "nio-acceptor-" + srvName;

            acceptWorker = new GridNioAcceptWorker(igniteInstanceName, threadName, log, acceptSelector, workerLsnr);
        }
        else {
            locAddr = null;
            acceptWorker = null;
        }

        clientWorkers = new ArrayList<>(selectorCnt);
        clientThreads = new IgniteThread[selectorCnt];

        for (int i = 0; i < selectorCnt; i++) {
            String threadName;

            if (srvName == null)
                threadName = "grid-nio-worker-" + i;
            else
                threadName = "grid-nio-worker-" + srvName + "-" + i;

            AbstractNioClientWorker worker = directMode ?
                new DirectNioClientWorker(i, igniteInstanceName, threadName, log, workerLsnr) :
                new ByteBufferNioClientWorker(i, igniteInstanceName, threadName, log, workerLsnr);

            clientWorkers.add(worker);

            clientThreads[i] = new IgniteThread(worker);

            clientThreads[i].setDaemon(daemon);
        }

        this.directMode = directMode;
        this.writerFactory = writerFactory;

        this.skipRecoveryPred = skipRecoveryPred != null ? skipRecoveryPred : F.alwaysFalse();

        long balancePeriod = IgniteSystemProperties.getLong(IgniteSystemProperties.IGNITE_IO_BALANCE_PERIOD, 5000);

        IgniteRunnable balancer0 = null;

        if (balancePeriod > 0) {
            boolean rndBalance = IgniteSystemProperties.getBoolean(IGNITE_IO_BALANCE_RANDOM_BALANCE, false);

            if (rndBalance)
                balancer0 = new RandomBalancer();
            else {
                balancer0 = readWriteSelectorsAssign ?
                    new ReadWriteSizeBasedBalancer(balancePeriod) :
                    new SizeBasedBalancer(balancePeriod);
            }
        }

        balancer = balancer0;

        this.mreg = mreg;

        rcvdBytesCntMetric = mreg == null ?
            null : mreg.longAdderMetric(RECEIVED_BYTES_METRIC_NAME, RECEIVED_BYTES_METRIC_DESC);

        sentBytesCntMetric = mreg == null ?
            null : mreg.longAdderMetric(SENT_BYTES_METRIC_NAME, SENT_BYTES_METRIC_DESC);

        outboundMessagesQueueSizeMetric = mreg == null ? null : mreg.longAdderMetric(
            OUTBOUND_MESSAGES_QUEUE_SIZE_METRIC_NAME,
            OUTBOUND_MESSAGES_QUEUE_SIZE_METRIC_DESC
        );
    }

    /**
     * @return Number of reader sessions move.
     */
    public long readerMoveCount() {
        return readerMoveCnt.get();
    }

    /**
     * @return Number of reader writer move.
     */
    public long writerMoveCount() {
        return writerMoveCnt.get();
    }

    /**
     * @return Configured port.
     */
    public int port() {
        return locAddr != null ? locAddr.getPort() : -1;
    }

    /**
     * Creates and returns a builder for a new instance of this class.
     *
     * @return Builder for new instance.
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Starts all associated threads to perform accept and read activities.
     */
    public void start() {
        filterChain.start();

        if (acceptWorker != null)
            new IgniteThread(acceptWorker).start();

        for (IgniteThread thread : clientThreads)
            thread.start();
    }

    /**
     * Stops all threads and releases all resources.
     */
    public void stop() {
        if (!closed) {
            closed = true;

            // Make sure to entirely stop acceptor if any.
            U.cancel(acceptWorker);
            U.join(acceptWorker, log);

            U.cancel(clientWorkers);
            U.join(clientWorkers, log);

            filterChain.stop();

            for (GridSelectorNioSessionImpl ses : sessions)
                ses.onServerStopped();
        }
    }

    /**
     * Gets the address server is bound to.
     *
     * @return Address server is bound to.
     */
    public InetSocketAddress localAddress() {
        return locAddr;
    }

    /**
     * @return Selector spins.
     */
    public long selectorSpins() {
        return selectorSpins;
    }

    /**
     * @param ses Session to close.
     * @param cause Optional close cause.
     * @return Future for operation.
     */
    public GridNioFuture<Boolean> close(GridNioSession ses, @Nullable IgniteCheckedException cause) {
        assert ses instanceof GridSelectorNioSessionImpl : ses;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        if (impl.closed())
            return new GridNioFinishedFuture<>(false);

        SessionCloseFuture fut = new SessionCloseFuture(impl, cause);

        impl.offerStateChange(fut);

        return fut;
    }

    /**
     * @param ses Session.
     */
    public void closeFromWorkerThread(GridNioSession ses) {
        assert ses instanceof GridSelectorNioSessionImpl : ses;

        GridSelectorNioSessionImpl ses0 = (GridSelectorNioSessionImpl)ses;

        ((AbstractNioClientWorker)ses0.worker()).close((GridSelectorNioSessionImpl)ses, null);
    }

    /**
     * @param ses Session.
     * @param msg Message.
     * @param createFut {@code True} if future should be created.
     * @param ackC Closure invoked when message ACK is received.
     * @return Future for operation.
     */
    private GridNioFuture<?> send(GridNioSession ses, Object msg, boolean createFut,
        IgniteInClosure<IgniteException> ackC) throws IgniteCheckedException {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        SessionWriteRequest req = createFut
            ? new SessionWriteFuture(impl, msg, ackC)
            : new SessionWriteRequestImpl(ses, msg, ackC);

        int msgCnt = impl.offerWrite(req);

        if (impl.closed()) {
            if (impl.removeWriteRequest(req)) {
                IOException err = new IOException("Failed to send message (connection was closed): " + impl);

                req.onError(err);

                if (!createFut)
                    throw new IgniteCheckedException(err);
            }
        }

        if (msgQueueLsnr != null)
            msgQueueLsnr.apply(impl, msgCnt);

        return createFut ? (GridNioFuture<?>)req : null;
    }



    /**
     * @return Sessions.
     */
    public Collection<? extends GridNioSession> sessions() {
        return sessions;
    }

    /**
     * @return Workers.
     */
    public List<AbstractNioClientWorker> workers() {
        return clientWorkers;
    }















    /**
     * @param ses Session.
     * @param from Move from index.
     * @param to Move to index.
     */
    private void move(GridNioSession ses, int from, int to) {
        assert ses instanceof GridSelectorNioSessionImpl;

        assert from >= 0 && from < clientWorkers.size() : from;
        assert to >= 0 && to < clientWorkers.size() : to;
        assert from != to;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;
        impl.offerStateChange(new SessionStartMoveRequest(impl, from, to));
    }

    /** */
    @NotNull private GridNioFuture<?> pauseReads(GridNioSession ses) {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        if (impl.closed())
            return new GridNioFinishedFuture<>(new IOException("Failed to pause reads " +
                "(connection was closed): " + impl));

        SessionOperationFuture<?> fut = new SessionOperationFuture<Void>(impl, NioOperation.PAUSE_READ);

        impl.offerStateChange(fut);

        return fut;
    }

    /** */
    @NotNull private GridNioFuture<?> resumeReads(GridNioSession ses) {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        if (impl.closed())
            return new GridNioFinishedFuture<>(new IOException("Failed to resume reads " +
                "(connection was closed): " + impl));

        SessionOperationFuture<?> fut = new SessionOperationFuture<Void>(impl, NioOperation.RESUME_READ);

        impl.offerStateChange(fut);

        return fut;
    }

    /**
     * Establishes a session.
     *
     * @param ch Channel to register within the server and create session for.
     * @param meta Optional meta for new session.
     * @param async Async connection.
     * @param lsnr Listener that should be invoked in NIO thread.
     * @return Future to get session.
     */
    public GridNioFuture<GridNioSession> createSession(
        final SocketChannel ch,
        @Nullable Map<Integer, Object> meta,
        boolean async,
        @Nullable IgniteInClosure<? super IgniteInternalFuture<GridNioSession>> lsnr
    ) {
        try {
            if (!closed) {
                ch.configureBlocking(false);

                assert !async || meta != null;

                SessionRegisterFuture req = new SessionRegisterFuture(async ? NioOperation.CONNECT : NioOperation.REGISTER, ch, meta, false);

                if (lsnr != null)
                    req.listen(lsnr);

                offer(req);

                return req;
            }
            else
                return new GridNioFinishedFuture<>(
                    new IgniteCheckedException("Failed to create session, server is stopped."));
        }
        catch (IOException e) {
            return new GridNioFinishedFuture<>(e);
        }
    }

    /**
     * @param ch Channel.
     * @param meta Session meta.
     */
    public GridNioFuture<GridNioSession> cancelConnect(final SocketChannel ch, Map<Integer, ?> meta) {
        if (!closed) {
            SessionRegisterFuture req = new SessionRegisterFuture(NioOperation.CANCEL_CONNECT, ch, meta, false);

            Integer idx = (Integer)meta.get(WORKER_IDX_META_KEY);

            assert idx != null : meta;

            clientWorkers.get(idx).offer(req);

            return req;
        }
        else
            return new GridNioFinishedFuture<>(new IgniteCheckedException("Failed to cancel connection, server is stopped."));
    }

    /**
     * @return Future.
     */
    public IgniteInternalFuture<String> dumpStats() {
        String msg = "NIO server statistics [readerSesBalanceCnt=" + readerMoveCnt.get() +
            ", writerSesBalanceCnt=" + writerMoveCnt.get() + ']';

        return dumpStats(msg, null);
    }

    /**
     * @param msg Message to add.
     * @param p Session predicate.
     * @return Future.
     */
    public IgniteInternalFuture<String> dumpStats(final String msg, IgnitePredicate<GridNioSession> p) {
        GridCompoundFuture<String, String> fut = new GridCompoundFuture<>(new IgniteReducer<String, String>() {
            private final StringBuilder sb = new StringBuilder(msg);

            @Override public boolean collect(@Nullable String msg) {
                if (!F.isEmpty(msg)) {
                    synchronized (sb) {
                        if (sb.length() > 0)
                            sb.append(U.nl());

                        sb.append(msg);
                    }
                }

                return true;
            }

            @Override public String reduce() {
                synchronized (sb) {
                    return sb.toString();
                }
            }
        });

        for (int i = 0; i < clientWorkers.size(); i++) {
            SessionOperationFuture<String> opFut = new DumpStatsFuture(p);

            clientWorkers.get(i).offer(opFut);

            fut.add(opFut);
        }

        fut.markInitialized();

        return fut;
    }

    /**
     * Gets configurable write timeout for this session. If not set, default value is {@link #DFLT_SES_WRITE_TIMEOUT}.
     *
     * @return Write timeout in milliseconds.
     */
    public long writeTimeout() {
        return writeTimeout;
    }

    /**
     * Sets configurable write timeout for session.
     *
     * @param writeTimeout Write timeout in milliseconds.
     */
    public void writeTimeout(long writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    /**
     * Gets configurable idle timeout for this session. If not set, default value is
     * {@link ConnectorConfiguration#DFLT_IDLE_TIMEOUT}.
     *
     * @return Idle timeout in milliseconds.
     */
    public long idleTimeout() {
        return idleTimeout;
    }

    /**
     * Sets configurable idle timeout for session.
     *
     * @param idleTimeout Idle timeout in milliseconds.
     */
    public void idleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;

        for (AbstractNioClientWorker worker : workers())
            worker.offer(new RecalculateIdleTimeoutRequest(null));
    }

    /**
     * Creates selector and binds server socket to a given address and port. If address is null
     * then will not bind any address and just creates a selector.
     *
     * @param addr Local address to listen on.
     * @return Created selector.
     * @throws IgniteCheckedException If selector could not be created or port is already in use.
     */
    private Selector createSelector(@Nullable SocketAddress addr) throws IgniteCheckedException {
        Selector selector = null;

        ServerSocketChannel srvrCh = null;

        try {
            // Create a new selector
            selector = SelectorProvider.provider().openSelector();

            if (addr != null) {
                // Create a new non-blocking server socket channel
                srvrCh = ServerSocketChannel.open();

                srvrCh.configureBlocking(false);

                if (sockRcvBuf > 0)
                    srvrCh.socket().setReceiveBufferSize(sockRcvBuf);

                // Bind the server socket to the specified address and port
                srvrCh.socket().bind(addr);

                // Register the server socket channel, indicating an interest in
                // accepting new connections
                srvrCh.register(selector, SelectionKey.OP_ACCEPT);
            }

            return selector;
        }
        catch (Throwable e) {
            U.close(srvrCh, log);
            U.close(selector, log);

            if (e instanceof Error)
                throw (Error)e;

            throw new IgniteCheckedException("Failed to initialize NIO selector.", e);
        }
    }

    /**
     * @param req Request to balance.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private synchronized void offer(SessionRegisterFuture req) {
        assert req.operation() == NioOperation.REGISTER || req.operation() == NioOperation.CONNECT : req;
        assert req.channel() != null : req;

        int workers = clientWorkers.size();

        int balanceIdx;

        if (workers > 1) {
            if (readWriteSelectorsAssign) {
                if (req.accepted()) {
                    balanceIdx = readBalanceIdx;

                    readBalanceIdx += 2;

                    if (readBalanceIdx >= workers)
                        readBalanceIdx = 0;
                }
                else {
                    balanceIdx = writeBalanceIdx;

                    writeBalanceIdx += 2;

                    if (writeBalanceIdx >= workers)
                        writeBalanceIdx = 1;
                }
            }
            else {
                balanceIdx = readBalanceIdx;

                readBalanceIdx++;

                if (readBalanceIdx >= workers)
                    readBalanceIdx = 0;
            }
        }
        else
            balanceIdx = 0;

        Map<Integer, Object> meta = (Map)req.meta();
        if (meta != null)
            meta.put(WORKER_IDX_META_KEY, balanceIdx);

        clientWorkers.get(balanceIdx).offer(req);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioServer.class, this);
    }

    /**
     * Gets outbound messages queue size.
     *
     * @return Write queue size.
     */
    public int outboundMessagesQueueSize() {
        if (outboundMessagesQueueSizeMetric == null)
            return -1;

        return (int) outboundMessagesQueueSizeMetric.value();
    }

    /**
     * Constructs a new instance of {@link GridNioServer}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Builder<T> {
        /** Empty filters. */
        private static final GridNioFilter[] EMPTY_FILTERS = new GridNioFilter[0];

        /** Local address. */
        private InetAddress addr;

        /** Local port. */
        private int port;

        /** Logger. */
        private IgniteLogger log;

        /** Selector count. */
        private int selectorCnt;

        /** Ignite instance name. */
        private String igniteInstanceName;

        /** TCP_NO_DELAY flag. */
        private boolean tcpNoDelay;

        /** Direct buffer flag. */
        private boolean directBuf;

        /** Byte order. */
        private ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;

        /** NIO server listener. */
        private GridNioServerListener<T> lsnr;

        /** Send buffer size. */
        private int sockSndBufSize;

        /** Receive buffer size. */
        private int sockRcvBufSize;

        /** Send queue limit. */
        private int sndQueueLimit = DFLT_SEND_QUEUE_LIMIT;

        /** Whether direct mode is used. */
        private boolean directMode;

        /** NIO filters. */
        private GridNioFilter[] filters;

        /** Idle timeout. */
        private long idleTimeout = -1;

        /** Write timeout. */
        private long writeTimeout = -1;

        /** Daemon flag. */
        private boolean daemon;

        /** Writer factory. */
        private GridNioMessageWriterFactory writerFactory;

        /** Skip recovery predicate. */
        private IgnitePredicate<Message> skipRecoveryPred;

        /** Message queue size listener. */
        private IgniteBiInClosure<GridNioSession, Integer> msgQueueLsnr;

        /** Name for threads identification. */
        private String srvName;

        /** */
        private long selectorSpins;

        /** */
        private boolean readWriteSelectorsAssign;

        /** Worker lifecycle listener to be used by server's worker threads. */
        private GridWorkerListener workerLsnr;

        /** Metrics registry. */
        private MetricRegistry mreg;

        /** Tracing processor */
        private Tracing tracing;

        /**
         * Finishes building the instance.
         *
         * @return Final instance of {@link GridNioServer}.
         * @throws IgniteCheckedException If NIO client worker creation failed or address is already in use.
         */
        public GridNioServer<T> build() throws IgniteCheckedException {
            GridNioServer<T> ret = new GridNioServer<>(
                addr,
                port,
                log,
                selectorCnt,
                igniteInstanceName,
                srvName,
                selectorSpins,
                tcpNoDelay,
                directBuf,
                byteOrder,
                lsnr,
                sockSndBufSize,
                sockRcvBufSize,
                sndQueueLimit,
                directMode,
                daemon,
                writerFactory,
                skipRecoveryPred,
                msgQueueLsnr,
                readWriteSelectorsAssign,
                workerLsnr,
                mreg,
                tracing,
                filters != null ? Arrays.copyOf(filters, filters.length) : EMPTY_FILTERS
            );

            if (idleTimeout >= 0)
                ret.idleTimeout(idleTimeout);

            if (writeTimeout >= 0)
                ret.writeTimeout(writeTimeout);

            return ret;
        }

        /**
         * @param readWriteSelectorsAssign {@code True} to assign in/out connections even/odd workers.
         * @return This for chaining.
         */
        public Builder<T> readWriteSelectorsAssign(boolean readWriteSelectorsAssign) {
            this.readWriteSelectorsAssign = readWriteSelectorsAssign;

            return this;
        }

        /**
         * @param tracing Tracing processor.
         * @return This for chaining.
         */
        public Builder<T> tracing(Tracing tracing) {
            this.tracing = tracing;

            return this;
        }

        /**
         * @param addr Local address.
         * @return This for chaining.
         */
        public Builder<T> address(InetAddress addr) {
            this.addr = addr;

            return this;
        }

        /**
         * @param port Local port. If {@code -1} passed then server will not be
         *      accepting connections and only outgoing connections will be possible.
         * @return This for chaining.
         */
        public Builder<T> port(int port) {
            this.port = port;

            return this;
        }

        /**
         * @param log Logger.
         * @return This for chaining.
         */
        public Builder<T> logger(IgniteLogger log) {
            this.log = log;

            return this;
        }

        /**
         * @param selectorCnt Selector count.
         * @return This for chaining.
         */
        public Builder<T> selectorCount(int selectorCnt) {
            this.selectorCnt = selectorCnt;

            return this;
        }

        /**
         * @param igniteInstanceName Ignite instance name.
         * @return This for chaining.
         */
        public Builder<T> igniteInstanceName(@Nullable String igniteInstanceName) {
            this.igniteInstanceName = igniteInstanceName;

            return this;
        }

        /**
         * @param srvName Logical server name for threads identification.
         * @return This for chaining.
         */
        public Builder<T> serverName(@Nullable String srvName) {
            this.srvName = srvName;

            return this;
        }

        /**
         * @param selectorSpins Defines how many non-blocking {@code selector.selectNow()} should be made before
         *      falling into {@code selector.select(long)} in NIO server. Long value. Default is {@code 0}.
         *      Can be set to {@code Long.MAX_VALUE} so selector threads will never block.
         * @return This for chaining.
         */
        public Builder<T> selectorSpins(long selectorSpins) {
            this.selectorSpins = selectorSpins;

            return this;
        }

        /**
         * @param tcpNoDelay If TCP_NODELAY option should be set to accepted sockets.
         * @return This for chaining.
         */
        public Builder<T> tcpNoDelay(boolean tcpNoDelay) {
            this.tcpNoDelay = tcpNoDelay;

            return this;
        }

        /**
         * @param directBuf Whether to use direct buffer.
         * @return This for chaining.
         */
        public Builder<T> directBuffer(boolean directBuf) {
            this.directBuf = directBuf;

            return this;
        }

        /**
         * @param byteOrder Byte order to use.
         * @return This for chaining.
         */
        public Builder<T> byteOrder(ByteOrder byteOrder) {
            this.byteOrder = byteOrder;

            return this;
        }

        /**
         * @param lsnr NIO server listener.
         * @return This for chaining.
         */
        public Builder<T> listener(GridNioServerListener<T> lsnr) {
            this.lsnr = lsnr;

            return this;
        }

        /**
         * @param sockSndBufSize Socket send buffer size.
         * @return This for chaining.
         */
        public Builder<T> socketSendBufferSize(int sockSndBufSize) {
            this.sockSndBufSize = sockSndBufSize;

            return this;
        }

        /**
         * @param sockRcvBufSize Socket receive buffer size.
         * @return This for chaining.
         */
        public Builder<T> socketReceiveBufferSize(int sockRcvBufSize) {
            this.sockRcvBufSize = sockRcvBufSize;

            return this;
        }

        /**
         * @param sndQueueLimit Send queue limit.
         * @return This for chaining.
         */
        public Builder<T> sendQueueLimit(int sndQueueLimit) {
            this.sndQueueLimit = sndQueueLimit;

            return this;
        }

        /**
         * @param directMode Whether direct mode is used.
         * @return This for chaining.
         */
        public Builder<T> directMode(boolean directMode) {
            this.directMode = directMode;

            return this;
        }

        /**
         * @param filters NIO filters.
         * @return This for chaining.
         */
        public Builder<T> filters(GridNioFilter... filters) {
            this.filters = filters;

            return this;
        }

        /**
         * @param idleTimeout Idle timeout.
         * @return This for chaining.
         */
        public Builder<T> idleTimeout(long idleTimeout) {
            this.idleTimeout = idleTimeout;

            return this;
        }

        /**
         * @param writeTimeout Write timeout.
         * @return This for chaining.
         */
        public Builder<T> writeTimeout(long writeTimeout) {
            this.writeTimeout = writeTimeout;

            return this;
        }

        /**
         * @param daemon Daemon flag to create threads.
         * @return This for chaining.
         */
        public Builder<T> daemon(boolean daemon) {
            this.daemon = daemon;

            return this;
        }

        /**
         * @param writerFactory Writer factory.
         * @return This for chaining.
         */
        public Builder<T> writerFactory(GridNioMessageWriterFactory writerFactory) {
            this.writerFactory = writerFactory;

            return this;
        }

        /**
         * @param skipRecoveryPred Skip recovery predicate.
         * @return This for chaining.
         */
        public Builder<T> skipRecoveryPredicate(IgnitePredicate<Message> skipRecoveryPred) {
            this.skipRecoveryPred = skipRecoveryPred;

            return this;
        }

        /**
         * @param msgQueueLsnr Message queue size listener.
         * @return Instance of this builder for chaining.
         */
        public Builder<T> messageQueueSizeListener(IgniteBiInClosure<GridNioSession, Integer> msgQueueLsnr) {
            this.msgQueueLsnr = msgQueueLsnr;

            return this;
        }

        /**
         * @param workerLsnr Worker lifecycle listener.
         * @return This for chaining.
         */
        public Builder<T> workerListener(GridWorkerListener workerLsnr) {
            this.workerLsnr = workerLsnr;

            return this;
        }

        /**
         * @param mreg Metrics registry.
         * @return This for chaining.
         */
        public Builder<T> metricRegistry(MetricRegistry mreg) {
            this.mreg = mreg;

            return this;
        }
    }

    /**
     * Thread performing only read operations from the channel.
     */
    private abstract class AbstractNioClientWorker extends GridWorker implements GridNioWorker {
        /** Queue of change requests on this selector. */
        @GridToStringExclude
        private final ConcurrentLinkedQueue<SessionOperationRequest> changeReqs = new ConcurrentLinkedQueue<>();

        /** Selector to select read events. */
        @GridToStringExclude
        private Selector selector;

        /** Selected keys. */
        @GridToStringExclude
        private SelectedSelectionKeySet selectedKeys;

        /** Worker index. */
        private final int idx;

        /** */
        private long bytesRcvd;

        /** */
        private long bytesSent;

        /** */
        private volatile long bytesRcvd0;

        /** */
        private volatile long bytesSent0;

        /** Sessions assigned to this worker. */
        @GridToStringExclude
        private final GridConcurrentHashSet<GridSelectorNioSessionImpl> workerSessions =
            new GridConcurrentHashSet<>();

        /** {@code True} if worker has called or is about to call {@code Selector.select()}. */
        private volatile boolean select;

        /** */
        private long idleTimeout;

        /**
         * @param idx Index of this worker in server's array.
         * @param igniteInstanceName Ignite instance name.
         * @param name Worker name.
         * @param log Logger.
         * @param workerLsnr Worker lifecycle listener.
         * @throws IgniteCheckedException If selector could not be created.
         */
        AbstractNioClientWorker(
            int idx,
            @Nullable String igniteInstanceName,
            String name,
            IgniteLogger log,
            @Nullable GridWorkerListener workerLsnr
        ) throws IgniteCheckedException {
            super(igniteInstanceName, name, log, workerLsnr);

            createSelector();

            this.idx = idx;

            idleTimeout = GridNioServer.this.idleTimeout;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                boolean reset = false;

                while (!closed) {
                    updateHeartbeat();

                    try {
                        if (reset)
                            createSelector();

                        bodyInternal();

                        onIdle();
                    }
                    catch (IgniteCheckedException e) {
                        if (!Thread.currentThread().isInterrupted()) {
                            U.error(log, "Failed to read data from remote connection (will wait for " +
                                ERR_WAIT_TIME + "ms).", e);

                            U.sleep(ERR_WAIT_TIME);

                            reset = true;
                        }
                    }
                }
            }
            catch (Throwable e) {
                U.error(log, "Caught unhandled exception in NIO worker thread (restart the node).", e);

                err = e;

                if (e instanceof Error)
                    throw e;
            }
            finally {
                if (err instanceof OutOfMemoryError)
                    lsnr.onFailure(CRITICAL_ERROR, err);
                else if (!closed) {
                    if (err == null)
                        lsnr.onFailure(SYSTEM_WORKER_TERMINATION,
                            new IllegalStateException("Thread " + name() + " is terminated unexpectedly"));
                    else
                        lsnr.onFailure(SYSTEM_WORKER_TERMINATION, err);
                }
                else if (err != null)
                    lsnr.onFailure(SYSTEM_WORKER_TERMINATION, err);
                else
                    // In case of closed == true, prevent general-case termination handling.
                    cancel();
            }
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void createSelector() throws IgniteCheckedException {
            selectedKeys = null;

            selector = GridNioServer.this.createSelector(null);

            if (DISABLE_KEYSET_OPTIMIZATION)
                return;

            try {
                SelectedSelectionKeySet selectedKeySet = new SelectedSelectionKeySet();

                Class<?> selectorImplCls =
                    Class.forName("sun.nio.ch.SelectorImpl", false, U.gridClassLoader());

                // Ensure the current selector implementation is what we can instrument.
                if (!selectorImplCls.isAssignableFrom(selector.getClass()))
                    return;

                Field selectedKeysField = selectorImplCls.getDeclaredField("selectedKeys");
                Field publicSelectedKeysField = selectorImplCls.getDeclaredField("publicSelectedKeys");

                selectedKeysField.setAccessible(true);
                publicSelectedKeysField.setAccessible(true);

                selectedKeysField.set(selector, selectedKeySet);
                publicSelectedKeysField.set(selector, selectedKeySet);

                selectedKeys = selectedKeySet;

                if (log.isDebugEnabled())
                    log.debug("Instrumented an optimized java.util.Set into: " + selector);
            }
            catch (Exception e) {
                selectedKeys = null;

                if (log.isDebugEnabled())
                    log.debug("Failed to instrument an optimized java.util.Set into selector [selector=" + selector
                        + ", err=" + e + ']');
            }
        }

        /** {@inheritDoc} */
        @Override public int index() {
            return idx;
        }

        /**
         * Adds socket channel to the registration queue and wakes up reading thread.
         *
         * @param req Change request.
         */
        @Override public void offer(SessionOperationRequest req) {
            if (log.isDebugEnabled())
                log.debug("The session change request was offered [req=" + req + "]");

            changeReqs.offer(req);

            if (select)
                selector.wakeup();
        }

        /** {@inheritDoc} */
        @Override public void offer(Collection<SessionOperationRequest> reqs) {
            if (log.isDebugEnabled()) {
                String strReqs = reqs.stream().map(Objects::toString).collect(Collectors.joining(","));

                log.debug("The session change requests were offered [reqs=" + strReqs + "]");
            }

            for (SessionOperationRequest req : reqs)
                changeReqs.offer(req);

            selector.wakeup();
        }

        /** {@inheritDoc} */
        @Override public List<SessionOperationRequest> clearSessionRequests(GridNioSession ses) {
            List<SessionOperationRequest> sesReqs = null;

            if (log.isDebugEnabled())
                log.debug("The session was removed [ses=" + ses + "]");

            for (SessionOperationRequest changeReq : changeReqs) {
                if (changeReq.session() == ses && !(changeReq instanceof SessionMoveRequest)) {
                    boolean rmv = changeReqs.remove(changeReq);

                    assert rmv : changeReq;

                    if (sesReqs == null)
                        sesReqs = new ArrayList<>();

                    sesReqs.add(changeReq);
                }
            }

            return sesReqs;
        }

        /**
         * Processes read and write events and registration requests.
         *
         * @throws IgniteCheckedException If IOException occurred or thread was unable to add worker to workers pool.
         */
        private void bodyInternal() throws IgniteCheckedException, InterruptedException {
            try {
                long lastIdleCheck = U.currentTimeMillis();

                mainLoop:
                while (!closed && selector.isOpen()) {
                    SessionOperationRequest req0;

                    updateHeartbeat();

                    while ((req0 = changeReqs.poll()) != null) {
                        updateHeartbeat();

                        if (log.isDebugEnabled())
                            log.debug("The session request will be processed [req=" + req0 + "]");

                        switch (req0.operation()) {
                            case CONNECT: {
                                SessionRegisterFuture fut = (SessionRegisterFuture)req0;

                                SocketChannel ch = fut.channel();

                                try {
                                    ch.register(selector, SelectionKey.OP_CONNECT, fut);
                                }
                                catch (IOException e) {
                                    fut.onDone(new IgniteCheckedException("Failed to register channel on selector", e));
                                }

                                break;
                            }

                            case CANCEL_CONNECT: {
                                SessionRegisterFuture req = (SessionRegisterFuture)req0;

                                SocketChannel ch = req.channel();

                                SelectionKey key = ch.keyFor(selector);

                                if (key != null)
                                    key.cancel();

                                U.closeQuiet(ch);

                                req.onDone();

                                break;
                            }

                            case REGISTER: {
                                SessionRegisterFuture req = (SessionRegisterFuture)req0;

                                register(req);

                                break;
                            }

                            case CLOSE: {
                                SessionCloseFuture req = (SessionCloseFuture)req0;

                                req.onDone(close(req.session(), req.cause()));

                                break;
                            }

                            case START_MOVE: {
                                SessionStartMoveRequest req = (SessionStartMoveRequest)req0;

                                if (req.fromIndex() != idx)
                                    break;

                                GridSelectorNioSessionImpl ses = req.session();
                                if (workerSessions.remove(ses)) {
                                    ses.startMoveSession(this);

                                    SelectionKey key = ses.key();

                                    SocketChannel channel = (SocketChannel)key.channel();

                                    assert channel != null : key;

                                    key.cancel();

                                    SessionOperationRequest newReq = new SessionFinishMoveRequest(ses, req.fromIndex(),
                                        req.toIndex(), channel);

                                    clientWorkers.get(req.toIndex()).offer(newReq);
                                }

                                break;
                            }

                            case FINISH_MOVE: {
                                SessionFinishMoveRequest req = (SessionFinishMoveRequest)req0;

                                assert idx == req.toIndex();

                                GridSelectorNioSessionImpl ses = req.session();
                                boolean add = workerSessions.add(ses);

                                assert add;

                                ses.finishMoveSession(this);

                                if (idx % 2 == 0)
                                    readerMoveCnt.incrementAndGet();
                                else
                                    writerMoveCnt.incrementAndGet();

                                SelectionKey key = req.channel().register(selector,
                                    SelectionKey.OP_READ | SelectionKey.OP_WRITE, ses);

                                ses.key(key);
                                ses.setWriting(true);

                                // Update timestamp to protected against false write timeout.
                                ses.bytesSent(0);

                                // Update timestamp to protected against false idle timeout.
                                ses.bytesReceived(0);

                                break;
                            }

                            case REQUIRE_WRITE: {
                                SessionWriteRequest req = (SessionWriteRequest)req0;

                                startWriting((GridSelectorNioSessionImpl)req.session());

                                break;
                            }

                            case PAUSE_READ: {
                                SessionOperationFuture<Boolean> req = (SessionOperationFuture<Boolean>)req0;

                                GridSelectorNioSessionImpl ses = req.session();
                                SelectionKey key = ses.key();

                                if (key.isValid()) {
                                    key.interestOps(key.interestOps() & (~SelectionKey.OP_READ));

                                    req.onDone(true);
                                }
                                else
                                    req.onDone(false);

                                break;
                            }

                            case RESUME_READ: {
                                SessionOperationFuture<Boolean> req = (SessionOperationFuture<Boolean>)req0;

                                GridSelectorNioSessionImpl ses = req.session();
                                SelectionKey key = ses.key();

                                if (key.isValid()) {
                                    key.interestOps(key.interestOps() | SelectionKey.OP_READ);

                                    // Update timestamp to protected against false idle timeout.
                                    ses.bytesReceived(0);

                                    req.onDone(true);
                                }
                                else
                                    req.onDone(false);

                                break;
                            }

                            case DUMP_STATS: {
                                DumpStatsFuture req = (DumpStatsFuture)req0;
                                StringBuilder sb = new StringBuilder();

                                try {
                                    dumpStats(sb, req.predicate(), req.predicate() != null);
                                }
                                finally {
                                    req.onDone(sb.toString());
                                }

                                break;
                            }

                            case RECALCULATE_IDLE_TIMEOUT: {
                                RecalculateIdleTimeoutRequest req = (RecalculateIdleTimeoutRequest)req0;

                                recalculateIdleTimeout(req.session(), GridNioServer.this.idleTimeout);

                                break;
                            }

                            case SESSION_OPERATION: {
                                SessionOperation req = (SessionOperation)req0;

                                req.run();

                                break;
                            }
                        }
                    }

                    int res = 0;

                    for (long i = 0; i < selectorSpins && res == 0; i++) {
                        res = selector.selectNow();

                        if (res > 0) {
                            // Walk through the ready keys collection and process network events.
                            updateHeartbeat();

                            if (selectedKeys == null)
                                processSelectedKeys(selector.selectedKeys());
                            else
                                processSelectedKeysOptimized(selectedKeys.flip());
                        }

                        if (!changeReqs.isEmpty())
                            continue mainLoop;

                        // Just in case we do busy selects.
                        long now = U.currentTimeMillis();

                        if (now - lastIdleCheck > idleTimeout) {
                            lastIdleCheck = now;

                            checkIdle(selector.keys());
                        }

                        if (isCancelled())
                            return;
                    }

                    // Falling to blocking select.
                    select = true;

                    try {
                        if (!changeReqs.isEmpty())
                            continue;

                        updateHeartbeat();

                        // Wake up every 2 seconds to check if closed.
                        if (selector.select(idleTimeout) > 0) {
                            // Walk through the ready keys collection and process network events.
                            if (selectedKeys == null)
                                processSelectedKeys(selector.selectedKeys());
                            else
                                processSelectedKeysOptimized(selectedKeys.flip());
                        }

                        // select() call above doesn't throw on interruption; checking it here to propagate timely.
                        if (!closed && !isCancelled && Thread.interrupted())
                            throw new InterruptedException();
                    }
                    finally {
                        select = false;
                    }

                    long now = U.currentTimeMillis();

                    if (now - lastIdleCheck > 2000) {
                        lastIdleCheck = now;

                        checkIdle(selector.keys());
                    }
                }
            }
            // Ignore this exception as thread interruption is equal to 'close' call.
            catch (ClosedByInterruptException e) {
                if (log.isDebugEnabled())
                    log.debug("Closing selector due to thread interruption: " + e.getMessage());
            }
            catch (ClosedSelectorException e) {
                throw new IgniteCheckedException("Selector got closed while active.", e);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to select events on selector.", e);
            }
            finally {
                if (selector.isOpen()) {
                    if (log.isDebugEnabled())
                        log.debug("Closing all connected client sockets.");

                    // Close all channels registered with selector.
                    for (SelectionKey key : selector.keys()) {
                        GridNioKeyAttachment attach = (GridNioKeyAttachment)key.attachment();

                        if (attach != null && attach.hasSession())
                            close(attach.session(), null);
                    }

                    if (log.isDebugEnabled())
                        log.debug("Closing NIO selector.");

                    U.close(selector, log);
                }
            }
        }

        private void recalculateIdleTimeout(GridNioSession session, long dfltIdleTimeout) {
            long sesIdleTimeout = sessionIdleTimeout(session, dfltIdleTimeout);

            if (sesIdleTimeout < idleTimeout)
                idleTimeout = sesIdleTimeout;
            else {
                long idleTimeout0 = dfltIdleTimeout;

                for (SelectionKey key : selector.keys()) {
                    GridNioKeyAttachment attach = (GridNioKeyAttachment)key.attachment();

                    if (attach == null || !attach.hasSession())
                        continue;

                    GridSelectorNioSessionImpl ses = attach.session();

                    sesIdleTimeout = sessionIdleTimeout(ses, dfltIdleTimeout);

                    if (sesIdleTimeout != GridNioSession.DEFAILT_IDLE_TIMEOUT)
                        idleTimeout0 = Math.min(idleTimeout0, sesIdleTimeout);
                }

                idleTimeout = idleTimeout0;
            }
        }

        /**
         * @param ses Session.
         */
        protected final void startWriting(GridSelectorNioSessionImpl ses) {
            SelectionKey key = ses.key();

            if (key.isValid()) {
                if ((key.interestOps() & SelectionKey.OP_WRITE) == 0)
                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);

                // Update timestamp to protected against false write timeout.
                ses.bytesSent(0);
            }
        }

        /**
         * Stop polling for write availability if write queue is empty.
         */
        protected final void stopWriting(SelectionKey key, GridSelectorNioSessionImpl ses) {
            if (!ses.setWriting(false))
                return;
            if (ses.writeQueue().isEmpty() && (key.interestOps() & SelectionKey.OP_WRITE) != 0)
                key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));
            else
                ses.setWriting(true);
        }

        /**
         * @param sb Message builder.
         * @param keys Keys.
         */
        private void dumpSelectorInfo(StringBuilder sb, Set<SelectionKey> keys) {
            sb.append(">> Selector info [id=").append(idx)
                .append(", keysCnt=").append(keys.size())
                .append(", bytesRcvd=").append(bytesRcvd)
                .append(", bytesRcvd0=").append(bytesRcvd0)
                .append(", bytesSent=").append(bytesSent)
                .append(", bytesSent0=").append(bytesSent0)
                .append("]").append(U.nl());
        }

        /**
         * @param sb Message builder.
         * @param p Optional session predicate.
         * @param shortInfo Short info flag.
         */
        private void dumpStats(StringBuilder sb,
            @Nullable IgnitePredicate<GridNioSession> p,
            boolean shortInfo) {
            Set<SelectionKey> keys = selector.keys();

            boolean selInfo = p == null;

            if (selInfo)
                dumpSelectorInfo(sb, keys);

            for (SelectionKey key : keys) {
                GridNioKeyAttachment attach = (GridNioKeyAttachment)key.attachment();

                if (!attach.hasSession())
                    continue;

                GridSelectorNioSessionImpl ses = attach.session();

                boolean sesInfo = p == null || p.apply(ses);

                if (sesInfo) {
                    if (!selInfo) {
                        dumpSelectorInfo(sb, keys);

                        selInfo = true;
                    }

                    sb.append("    Connection info [")
                        .append("in=").append(ses.accepted())
                        .append(", rmtAddr=").append(ses.remoteAddress())
                        .append(", locAddr=").append(ses.localAddress());

                    GridNioRecoveryDescriptor outDesc = ses.outRecoveryDescriptor();

                    if (outDesc != null) {
                        sb.append(", msgsSent=").append(outDesc.sent())
                            .append(", msgsAckedByRmt=").append(outDesc.acked())
                            .append(", descIdHash=").append(System.identityHashCode(outDesc));

                        if (!outDesc.messagesRequests().isEmpty()) {
                            int cnt = 0;

                            sb.append(", unackedMsgs=[");

                            for (SessionWriteRequest req : outDesc.messagesRequests()) {
                                if (cnt != 0)
                                    sb.append(", ");

                                Object msg = req.message();

                                if (shortInfo && msg instanceof GridIoMessage)
                                    msg = ((GridIoMessage)msg).message().getClass().getSimpleName();

                                sb.append(msg);

                                if (++cnt == 5)
                                    break;
                            }

                            sb.append(']');
                        }
                    }
                    else
                        sb.append(", outRecoveryDesc=null");

                    GridNioRecoveryDescriptor inDesc = ses.inRecoveryDescriptor();

                    if (inDesc != null) {
                        sb.append(", msgsRcvd=").append(inDesc.received())
                            .append(", lastAcked=").append(inDesc.lastAcknowledged())
                            .append(", descIdHash=").append(System.identityHashCode(inDesc));
                    }
                    else
                        sb.append(", inRecoveryDesc=null");

                    sb.append(", bytesRcvd=").append(ses.bytesReceived())
                        .append(", bytesRcvd0=").append(ses.bytesReceived0())
                        .append(", bytesSent=").append(ses.bytesSent())
                        .append(", bytesSent0=").append(ses.bytesSent0())
                        .append(", opQueueSize=").append(ses.writeQueueSize());

                    if (!shortInfo) {
                        MessageWriter writer = ses.meta(MSG_WRITER.ordinal());
                        MessageReader reader = ses.meta(GridDirectParser.READER_META_KEY);

                        sb.append(", msgWriter=").append(writer != null ? writer.toString() : "null")
                            .append(", msgReader=").append(reader != null ? reader.toString() : "null");
                    }

                    int cnt = 0;

                    for (SessionWriteRequest req : ses.writeQueue()) {
                        Object msg = req.message();

                        if (shortInfo && msg instanceof GridIoMessage)
                            msg = ((GridIoMessage)msg).message().getClass().getSimpleName();

                        if (cnt == 0)
                            sb.append(",\n opQueue=[").append(msg);
                        else
                            sb.append(',').append(msg);

                        if (++cnt == 5) {
                            sb.append(']');

                            break;
                        }
                    }

                    sb.append("]");
                }
            }
        }

        /**
         * Processes keys selected by a selector.
         *
         * @param keys Selected keys.
         * @throws ClosedByInterruptException If this thread was interrupted while reading data.
         */
        private void processSelectedKeysOptimized(SelectionKey[] keys) throws ClosedByInterruptException {
            for (int i = 0; ; i++) {
                final SelectionKey key = keys[i];

                if (key == null)
                    break;

                // null out entry in the array to allow to have it GC'ed once the Channel close
                // See https://github.com/netty/netty/issues/2363
                keys[i] = null;

                // Was key closed?
                if (!key.isValid())
                    continue;

                GridNioKeyAttachment attach = (GridNioKeyAttachment)key.attachment();

                assert attach != null;

                try {
                    if (!attach.hasSession() && key.isConnectable()) {
                        processConnect(key);

                        continue;
                    }

                    if (key.isReadable())
                        processRead(key);

                    if (key.isValid() && key.isWritable())
                        processWrite(key);
                }
                catch (ClosedByInterruptException e) {
                    // This exception will be handled in bodyInternal() method.
                    throw e;
                }
                catch (Exception | Error e) { // TODO IGNITE-2659.
                    try {
                        U.sleep(1000);
                    }
                    catch (IgniteInterruptedCheckedException ignore) {
                        // No-op.
                    }

                    GridSelectorNioSessionImpl ses = attach.session();

                    if (!closed)
                        U.error(log, "Failed to process selector key [ses=" + ses + ']', e);
                    else if (log.isDebugEnabled())
                        log.debug("Failed to process selector key [ses=" + ses + ", err=" + e + ']');

                    // Can be null if async connect failed.
                    if (ses != null)
                        close(ses, new GridNioException(e));
                    else
                        closeKey(key);
                }
            }
        }

        /**
         * Processes keys selected by a selector.
         *
         * @param keys Selected keys.
         * @throws ClosedByInterruptException If this thread was interrupted while reading data.
         */
        private void processSelectedKeys(Set<SelectionKey> keys) throws ClosedByInterruptException {
            if (log.isTraceEnabled())
                log.trace("Processing keys in client worker: " + keys.size());

            if (keys.isEmpty())
                return;

            for (Iterator<SelectionKey> iter = keys.iterator(); iter.hasNext(); ) {
                SelectionKey key = iter.next();

                iter.remove();

                // Was key closed?
                if (!key.isValid())
                    continue;

                GridNioKeyAttachment attach = (GridNioKeyAttachment)key.attachment();

                assert attach != null;

                try {
                    if (!attach.hasSession() && key.isConnectable()) {
                        processConnect(key);

                        continue;
                    }

                    if (key.isReadable())
                        processRead(key);

                    if (key.isValid() && key.isWritable())
                        processWrite(key);
                }
                catch (ClosedByInterruptException e) {
                    // This exception will be handled in bodyInternal() method.
                    throw e;
                }
                catch (Exception | Error e) { // TODO IGNITE-2659.
                    try {
                        U.sleep(1000);
                    }
                    catch (IgniteInterruptedCheckedException ignore) {
                        // No-op.
                    }

                    GridSelectorNioSessionImpl ses = attach.session();

                    if (!closed)
                        U.error(log, "Failed to process selector key [ses=" + ses + ']', e);
                    else if (log.isDebugEnabled())
                        log.debug("Failed to process selector key [ses=" + ses + ", err=" + e + ']');
                }
            }
        }

        /**
         * Checks sessions assigned to a selector for timeouts.
         *
         * @param keys Keys registered to selector.
         */
        private void checkIdle(Iterable<SelectionKey> keys) {
            long now = U.currentTimeMillis();

            for (SelectionKey key : keys) {
                GridNioKeyAttachment attach = (GridNioKeyAttachment)key.attachment();

                if (attach == null || !attach.hasSession())
                    continue;

                GridSelectorNioSessionImpl ses = attach.session();

                try {
                    long writeTimeout0 = writeTimeout;
                    long idleTimeout0 = sessionIdleTimeout(ses, GridNioServer.this.idleTimeout);

                    boolean opWrite = key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) != 0;
                    boolean opRead = key.isValid() && (key.interestOps() & SelectionKey.OP_READ) != 0;

                    // If we are writing and timeout passed.
                    boolean notifyWriteTimeout = opWrite
                        && now - ses.lastSendTime() > writeTimeout0;

                    if (!opRead && !opWrite)
                        continue;

                    boolean notifyIdleTimeout = now - ses.lastSendScheduleTime() > idleTimeout0
                        && (!opRead || now - ses.lastReceiveTime() > idleTimeout0)
                        && (!opWrite || now - ses.lastSendTime() > idleTimeout0);

                    if (notifyWriteTimeout) {
                        filterChain.onSessionWriteTimeout(ses);
                        // Update timestamp to avoid multiple notifications within one timeout interval.
                        ses.bytesSent(0);
                    }

                    if (notifyIdleTimeout) {
                        filterChain.onSessionIdleTimeout(ses);

                        // Update timestamp to avoid multiple notifications within one timeout interval.
                        ses.bytesReceived(0);
                    }
                }
                catch (IgniteCheckedException e) {
                    close(ses, e);
                }
            }
        }

        /** */
        private long sessionIdleTimeout(GridNioSession ses, long dfltIdleTimeout) {
            if (ses == null)
                return dfltIdleTimeout;

            long sesIdleTimeout = ses.idleTimeout();

            return sesIdleTimeout == GridNioSession.DEFAILT_IDLE_TIMEOUT
                ? dfltIdleTimeout : sesIdleTimeout;
        }

        /**
         * Registers given socket channel to the selector, creates a session and notifies the listener.
         *
         * @param fut Registration future.
         */
        private void register(SessionRegisterFuture fut) {
            assert fut != null;

            SocketChannel sockCh = fut.channel();

            assert sockCh != null;

            Socket sock = sockCh.socket();

            try {
                ByteBuffer writeBuf = null;
                ByteBuffer readBuf = null;

                if (directMode) {
                    writeBuf = directBuf ? ByteBuffer.allocateDirect(sock.getSendBufferSize()) :
                        ByteBuffer.allocate(sock.getSendBufferSize());
                    readBuf = directBuf ? ByteBuffer.allocateDirect(sock.getReceiveBufferSize()) :
                        ByteBuffer.allocate(sock.getReceiveBufferSize());

                    writeBuf.order(order);
                    readBuf.order(order);
                }

                final GridSelectorNioSessionImpl ses = new GridSelectorNioSessionImpl(
                    log,
                    this,
                    filterChain,
                    (InetSocketAddress)sockCh.getLocalAddress(),
                    (InetSocketAddress)sockCh.getRemoteAddress(),
                    fut.accepted(),
                    sndQueueLimit,
                    mreg,
                    writeBuf,
                    readBuf);

                Map<Integer, ?> meta = fut.meta();

                if (meta != null) {
                    for (Entry<Integer, ?> e : meta.entrySet())
                        ses.addMeta(e.getKey(), e.getValue());

                    if (!ses.accepted()) {
                        GridNioRecoveryDescriptor desc =
                            (GridNioRecoveryDescriptor)meta.get(RECOVERY_DESC_META_KEY);

                        if (desc != null) {
                            ses.outRecoveryDescriptor(desc);

                            if (!desc.pairedConnections())
                                ses.inRecoveryDescriptor(desc);
                        }
                    }
                }

                SelectionKey key;
                if (!sockCh.isRegistered()) {
                    assert fut.operation() == NioOperation.REGISTER : fut.operation();

                    key = sockCh.register(selector, SelectionKey.OP_READ, ses);

                }
                else {
                    assert fut.operation() == NioOperation.CONNECT : fut.operation();

                    key = sockCh.keyFor(selector);

                    key.attach(ses);

                    key.interestOps(key.interestOps() & (~SelectionKey.OP_CONNECT));
                    key.interestOps(key.interestOps() | SelectionKey.OP_READ);

                }

                ses.key(key);

                sessions.add(ses);
                workerSessions.add(ses);

                try {
                    filterChain.onSessionOpened(ses);

                    fut.onDone(ses);
                }
                catch (IgniteCheckedException e) {
                    close(ses, e);

                    fut.onDone(e);
                }

                if (closed)
                    ses.onServerStopped();
            }
            catch (ClosedChannelException e) {
                U.warn(log, "Failed to register accepted socket channel to selector (channel was closed): "
                    + sock.getRemoteSocketAddress(), e);

                fut.onDone(e);
            }
            catch (IOException e) {
                U.error(log, "Failed to get socket addresses.", e);

                fut.onDone(e);
            }
            catch (RuntimeException | Error e) {
                fut.onDone(e); // any other errors

                throw e;
            }
        }

        /**
         * @param key Key.
         */
        private void closeKey(SelectionKey key) {
            // Shutdown input and output so that remote client will see correct socket close.
            Socket sock = ((SocketChannel)key.channel()).socket();

            try {
                try {
                    sock.shutdownInput();
                }
                catch (IOException ignored) {
                    // No-op.
                }

                try {
                    sock.shutdownOutput();
                }
                catch (IOException ignored) {
                    // No-op.
                }
            }
            finally {
                U.close(key, log);
                U.close(sock, log);
            }
        }

        /**
         * Closes the session and all associated resources, then notifies the listener.
         *
         * @param ses Session to be closed.
         * @param e Exception to be passed to the listener, if any.
         * @return {@code True} if this call closed the ses.
         */
        protected boolean close(final GridSelectorNioSessionImpl ses, @Nullable final IgniteCheckedException e) {
            if (e != null) {
                // Print stack trace only if has runtime exception in it's cause.
                if (e.hasCause(IOException.class))
                    U.warn(log, "Client disconnected abruptly due to network connection loss or because " +
                        "the connection was left open on application shutdown. [cls=" + e.getClass() +
                        ", msg=" + e.getMessage() + ']');
                else
                    U.error(log, "Closing NIO session because of unhandled exception.", e);
            }

            sessions.remove(ses);
            workerSessions.remove(ses);

            if (ses.setClosed()) {
                ses.onClosed();

                if (directBuf) {
                    if (ses.writeBuffer() != null)
                        GridUnsafe.cleanDirectBuffer(ses.writeBuffer());

                    if (ses.readBuffer() != null)
                        GridUnsafe.cleanDirectBuffer(ses.readBuffer());
                }

                closeKey(ses.key());

                if (e != null)
                    filterChain.onExceptionCaught(ses, e);

                ses.removeMeta(BUF_META_KEY);

                try {
                    filterChain.onSessionClosed(ses);
                }
                catch (IgniteCheckedException e1) {
                    filterChain.onExceptionCaught(ses, e1);
                }

                return true;
            }

            return false;
        }

        /**
         * @param key Key.
         * @throws IOException If failed.
         */
        private void processConnect(SelectionKey key) throws IOException {
            SocketChannel ch = (SocketChannel)key.channel();

            SessionRegisterFuture sesFut = (SessionRegisterFuture)key.attachment();

            assert sesFut != null;

            try {
                if (ch.finishConnect())
                    register(sesFut);
            }
            catch (IOException e) {
                U.closeQuiet(ch);

                sesFut.onDone(new GridNioException("Failed to connect to node", e));

                throw e;
            }
        }

        /**
         * Processes read-available event on the key.
         *
         * @param key Key that is ready to be read.
         * @throws IOException If key read failed.
         */
        protected abstract void processRead(SelectionKey key) throws IOException;

        /**
         * Processes write-ready event on the key.
         *
         * @param key Key that is ready to be written.
         * @throws IOException If write failed.
         */
        protected abstract void processWrite(SelectionKey key) throws IOException;

        /**
         * @param cnt Bytes received.
         */
        final void onRead(int cnt) {
            bytesRcvd += cnt;
            bytesRcvd0 += cnt;
        }

        /**
         * @param cnt Bytes sent.
         */
        final void onWrite(int cnt) {
            bytesSent += cnt;
            bytesSent0 += cnt;
        }

        /**
         *
         */
        final void reset0() {
            bytesSent0 = 0;
            bytesRcvd0 = 0;

            for (GridSelectorNioSessionImpl ses : workerSessions)
                ses.reset0();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AbstractNioClientWorker.class, this, super.toString());
        }
    }

    /**
     * Client worker for direct mode.
     */
    private class DirectNioClientWorker extends AbstractNioClientWorker {
        /**
         * @param idx Index of this worker in server's array.
         * @param igniteInstanceName Ignite instance name.
         * @param name Worker name.
         * @param log Logger.
         * @param workerLsnr Worker lifecycle listener.
         * @throws IgniteCheckedException If selector could not be created.
         */
        protected DirectNioClientWorker(
            int idx,
            @Nullable String igniteInstanceName,
            String name,
            IgniteLogger log,
            @Nullable GridWorkerListener workerLsnr
        ) throws IgniteCheckedException {
            super(idx, igniteInstanceName, name, log, workerLsnr);
        }

        /**
         * Processes read-available event on the key.
         *
         * @param key Key that is ready to be read.
         * @throws IOException If key read failed.
         */
        @Override protected void processRead(SelectionKey key) throws IOException {
            if (skipRead) {
                try {
                    U.sleep(50);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    U.warn(log, "Sleep has been interrupted.");
                }

                return;
            }

            ReadableByteChannel sockCh = (ReadableByteChannel)key.channel();

            final GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

            ByteBuffer readBuf = ses.readBuffer();

            // Attempt to read off the channel.
            int cnt = sockCh.read(readBuf);

            if (cnt == -1) {
                if (log.isDebugEnabled())
                    log.debug("Remote client closed connection: " + ses);

                close(ses, null);

                return;
            }

            if (log.isTraceEnabled())
                log.trace("Bytes received [sockCh=" + sockCh + ", cnt=" + cnt + ']');

            if (cnt == 0)
                return;

            if (rcvdBytesCntMetric != null)
                rcvdBytesCntMetric.add(cnt);

            ses.bytesReceived(cnt);
            onRead(cnt);

            readBuf.flip();

            assert readBuf.hasRemaining();

            try {
                filterChain.onMessageReceived(ses, readBuf);

                if (readBuf.hasRemaining())
                    readBuf.compact();
                else
                    readBuf.clear();
            }
            catch (IgniteCheckedException e) {
                close(ses, e);
            }
        }

        /**
         * Processes write-ready event on the key.
         *
         * @param key Key that is ready to be written.
         * @throws IOException If write failed.
         */
        @Override protected void processWrite(SelectionKey key) throws IOException {
            if (sslFilter != null)
                processWriteSsl(key);
            else
                processWrite0(key);
        }

        /**
         * Processes write-ready event on the key.
         *
         * @param key Key that is ready to be written.
         * @throws IOException If write failed.
         */
        private void processWriteSsl(SelectionKey key) throws IOException {
            WritableByteChannel sockCh = (WritableByteChannel)key.channel();

            GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

            MessageWriter writer = ses.meta(MSG_WRITER.ordinal());

            if (writer == null) {
                try {
                    ses.addMeta(MSG_WRITER.ordinal(), writer = writerFactory.writer(ses));
                }
                catch (IgniteCheckedException e) {
                    throw new IOException("Failed to create message writer.", e);
                }
            }

            boolean handshakeFinished = sslFilter.lock(ses);

            try {
                boolean writeFinished = writeSslSystem(ses, sockCh);

                if (!handshakeFinished) {
                    if (writeFinished)
                        stopWriting(key, ses);

                    return;
                }

                ByteBuffer sslNetBuf = ses.removeMeta(BUF_META_KEY);

                if (sslNetBuf != null) {
                    int cnt = sockCh.write(sslNetBuf);

                    if (sentBytesCntMetric != null)
                        sentBytesCntMetric.add(cnt);

                    ses.bytesSent(cnt);

                    if (sslNetBuf.hasRemaining()) {
                        ses.addMeta(BUF_META_KEY, sslNetBuf);

                        return;
                    }
                    else {
                        List<SessionWriteRequest> requests = ses.removeMeta(REQUESTS_META_KEY);

                        if (!F.isEmpty(requests)) {
                            try {
                                for (SessionWriteRequest request : requests) {
                                    request.onMessageWritten();
                                    filterChain.onMessageSent(ses, request.message());
                                }
                            }
                            catch (IgniteCheckedException e) {
                                close(ses, e);
                            }
                        }
                    }
                }

                ByteBuffer buf = ses.writeBuffer();

                if (ses.meta(WRITE_BUF_LIMIT) != null)
                    buf.limit(ses.meta(WRITE_BUF_LIMIT));

                SessionWriteRequest req = ses.removeMeta(NIO_OPERATION.ordinal());

                while (true) {
                    if (req == null) {
                        req = ses.pollRequest();

                        if (req == null && buf.position() == 0) {
                            stopWriting(key, ses);

                            break;
                        }
                    }

                    boolean finished = false;

                    List<SessionWriteRequest> pendingRequests = new ArrayList<>(2);

                    if (req != null)
                        finished = writeToBuffer(writer, buf, req, pendingRequests);

                    // Fill up as many messages as possible to write buffer.
                    while (finished) {
                        req = ses.pollRequest();

                        if (req == null)
                            break;

                        finished = writeToBuffer(writer, buf, req, pendingRequests);
                    }

                    int sesBufLimit = buf.limit();
                    int sesCap = buf.capacity();

                    buf.flip();

                    buf = sslFilter.encrypt(ses, buf);

                    ByteBuffer sesBuf = ses.writeBuffer();

                    sesBuf.clear();

                    if (sesCap - buf.limit() < 0) {
                        int limit = sesBufLimit + (sesCap - buf.limit()) - 100;

                        ses.addMeta(WRITE_BUF_LIMIT, limit);

                        sesBuf.limit(limit);
                    }

                    assert buf.hasRemaining();

                    if (!skipWrite) {
                        int cnt = sockCh.write(buf);

                        if (log.isTraceEnabled())
                            log.trace("Bytes sent [sockCh=" + sockCh + ", cnt=" + cnt + ']');

                        if (sentBytesCntMetric != null)
                            sentBytesCntMetric.add(cnt);

                        ses.bytesSent(cnt);
                    }
                    else {
                        // For test purposes only (skipWrite is set to true in tests only).
                        try {
                            U.sleep(50);
                        }
                        catch (IgniteInterruptedCheckedException e) {
                            throw new IOException("Thread has been interrupted.", e);
                        }
                    }

                    ses.addMeta(NIO_OPERATION.ordinal(), req);

                    if (buf.hasRemaining()) {
                        ses.addMeta(BUF_META_KEY, buf);

                        ses.addMeta(REQUESTS_META_KEY, pendingRequests);

                        break;
                    }
                    else {
                        try {
                            for (SessionWriteRequest request : pendingRequests) {
                                request.onMessageWritten();
                                filterChain.onMessageSent(ses, request.message());
                            }
                        }
                        catch (IgniteCheckedException e) {
                            close(ses, e);
                        }

                        buf = ses.writeBuffer();

                        if (ses.meta(WRITE_BUF_LIMIT) != null)
                            buf.limit(ses.meta(WRITE_BUF_LIMIT));
                    }
                }
            }
            finally {
                sslFilter.unlock(ses);
            }
        }

        /**
         * @param writer Customizer of writing.
         * @param buf Buffer to write.
         * @param req Source of data.
         * @param pendingRequests List of requests which was successfully written.
         * @return {@code true} if message successfully written to buffer and {@code false} otherwise.
         */
        private boolean writeToBuffer(
            MessageWriter writer,
            ByteBuffer buf,
            SessionWriteRequest req,
            List<SessionWriteRequest> pendingRequests
        ) {
            Message msg;
            boolean finished;
            msg = (Message)req.message();

            Span span = tracing.create(SpanType.COMMUNICATION_SOCKET_WRITE, req.span());

            try (TraceSurroundings ignore = span.equals(NoopSpan.INSTANCE) ? null : MTC.support(span)) {
                MTC.span().addTag(SpanTags.MESSAGE, () -> traceName(msg));

                assert msg != null;

                if (writer != null)
                    writer.setCurrentWriteClass(msg.getClass());

                finished = msg.writeTo(buf, writer);

                if (finished) {
                    pendingRequests.add(req);

                    if (writer != null)
                        writer.reset();
                }

                return finished;
            }
        }

        /**
         * @param ses NIO session.
         * @param sockCh Socket channel.
         * @throws IOException If failed.
         *
         * @return {@code True} if there's nothing else to write (last buffer is written and queue is empty).
         */
        private boolean writeSslSystem(GridSelectorNioSessionImpl ses, WritableByteChannel sockCh)
            throws IOException {
            ConcurrentLinkedQueue<ByteBuffer> queue = ses.meta(BUF_SSL_SYSTEM_META_KEY);

            assert queue != null;

            ByteBuffer buf;

            while ((buf = queue.peek()) != null) {
                int cnt = sockCh.write(buf);

                if (sentBytesCntMetric != null)
                    sentBytesCntMetric.add(cnt);

                ses.bytesSent(cnt);

                if (!buf.hasRemaining())
                    queue.poll();
                else
                    return false;
            }

            return true;
        }

        /**
         * Processes write-ready event on the key.
         *
         * @param key Key that is ready to be written.
         * @throws IOException If write failed.
         */
        private void processWrite0(SelectionKey key) throws IOException {
            WritableByteChannel sockCh = (WritableByteChannel)key.channel();

            GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();
            ByteBuffer buf = ses.writeBuffer();
            SessionWriteRequest req = ses.removeMeta(NIO_OPERATION.ordinal());

            MessageWriter writer = ses.meta(MSG_WRITER.ordinal());

            if (writer == null) {
                try {
                    ses.addMeta(MSG_WRITER.ordinal(), writer = writerFactory.writer(ses));
                }
                catch (IgniteCheckedException e) {
                    throw new IOException("Failed to create message writer.", e);
                }
            }

            if (req == null) {
                req = ses.pollRequest();

                if (req == null && buf.position() == 0) {
                    stopWriting(key, ses);

                    return;
                }
            }

            boolean finished = false;

            if (req != null)
                finished = writeToBuffer(buf, req, writer);

            try {
                // Fill up as many messages as possible to write buffer.
                while (finished) {
                    req.onMessageWritten();
                    filterChain.onMessageSent(ses, req.message());

                    req = ses.pollRequest();

                    if (req == null)
                        break;

                    finished = writeToBuffer(buf, req, writer);
                }
            }
            catch (IgniteCheckedException e) {
                close(ses, e);
            }

            buf.flip();

            assert buf.hasRemaining();

            if (!skipWrite) {
                int cnt = sockCh.write(buf);

                if (log.isTraceEnabled())
                    log.trace("Bytes sent [sockCh=" + sockCh + ", cnt=" + cnt + ']');

                if (sentBytesCntMetric != null)
                    sentBytesCntMetric.add(cnt);

                ses.bytesSent(cnt);
                onWrite(cnt);
            }
            else {
                // For test purposes only (skipWrite is set to true in tests only).
                try {
                    U.sleep(50);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IOException("Thread has been interrupted.", e);
                }
            }

            if (buf.hasRemaining() || !finished) {
                buf.compact();

                ses.addMeta(NIO_OPERATION.ordinal(), req);
            }
            else
                buf.clear();
        }

        /**
         * @param writer Customizer of writing.
         * @param buf Buffer to write.
         * @param req Source of data.
         * @return {@code true} if message successfully written to buffer and {@code false} otherwise.
         */
        private boolean writeToBuffer(ByteBuffer buf, SessionWriteRequest req, MessageWriter writer) {
            Message msg;
            boolean finished;
            msg = (Message)req.message();

            assert msg != null : req;

            Span span = tracing.create(SpanType.COMMUNICATION_SOCKET_WRITE, req.span());

            try (TraceSurroundings ignore = span.equals(NoopSpan.INSTANCE) ? null : MTC.support(span)) {
                MTC.span().addTag(SpanTags.MESSAGE, () -> traceName(msg));

                if (writer != null)
                    writer.setCurrentWriteClass(msg.getClass());

                finished = msg.writeTo(buf, writer);

                if (finished) {
                    if (writer != null)
                        writer.reset();
                }

                return finished;
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DirectNioClientWorker.class, this, super.toString());
        }
    }

    /**
     * Client worker for byte buffer mode.
     */
    private class ByteBufferNioClientWorker extends AbstractNioClientWorker {
        /** Read buffer. */
        private final ByteBuffer readBuf;

        /**
         * @param idx Index of this worker in server's array.
         * @param igniteInstanceName Ignite instance name.
         * @param name Worker name.
         * @param log Logger.
         * @param workerLsnr Worker lifecycle listener.
         * @throws IgniteCheckedException If selector could not be created.
         */
        protected ByteBufferNioClientWorker(
            int idx,
            @Nullable String igniteInstanceName,
            String name,
            IgniteLogger log,
            @Nullable GridWorkerListener workerLsnr
        ) throws IgniteCheckedException {
            super(idx, igniteInstanceName, name, log, workerLsnr);

            readBuf = directBuf ? ByteBuffer.allocateDirect(8 << 10) : ByteBuffer.allocate(8 << 10);

            readBuf.order(order);
        }

        /**
         * Processes read-available event on the key.
         *
         * @param key Key that is ready to be read.
         * @throws IOException If key read failed.
         */
        @Override protected void processRead(SelectionKey key) throws IOException {
            if (skipRead) {
                try {
                    U.sleep(50);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    U.warn(log, "Sleep has been interrupted.");
                }

                return;
            }

            ReadableByteChannel sockCh = (ReadableByteChannel)key.channel();

            final GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

            // Reset buffer to read bytes up to its capacity.
            readBuf.clear();

            // Attempt to read off the channel
            int cnt = sockCh.read(readBuf);

            if (cnt == -1) {
                if (log.isDebugEnabled())
                    log.debug("Remote client closed connection: " + ses);

                close(ses, null);

                return;
            }
            else if (cnt == 0)
                return;

            if (log.isTraceEnabled())
                log.trace("Bytes received [sockCh=" + sockCh + ", cnt=" + cnt + ']');

            if (rcvdBytesCntMetric != null)
                rcvdBytesCntMetric.add(cnt);

            ses.bytesReceived(cnt);

            // Sets limit to current position and
            // resets position to 0.
            readBuf.flip();

            try {
                assert readBuf.hasRemaining();

                filterChain.onMessageReceived(ses, readBuf);

                if (readBuf.remaining() > 0) {
                    LT.warn(log, "Read buffer contains data after filter chain processing (will discard " +
                        "remaining bytes) [ses=" + ses + ", remainingCnt=" + readBuf.remaining() + ']');

                    readBuf.clear();
                }
            }
            catch (IgniteCheckedException e) {
                close(ses, e);
            }
        }

        /**
         * Processes write-ready event on the key.
         *
         * @param key Key that is ready to be written.
         * @throws IOException If write failed.
         */
        @Override protected void processWrite(SelectionKey key) throws IOException {
            WritableByteChannel sockCh = (WritableByteChannel)key.channel();

            final GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

            while (true) {
                ByteBuffer buf = ses.removeMeta(BUF_META_KEY);
                SessionWriteRequest req = ses.removeMeta(NIO_OPERATION.ordinal());

                // Check if there were any pending data from previous writes.
                if (buf == null) {
                    assert req == null;

                    req = ses.pollRequest();

                    if (req == null) {
                        stopWriting(key, ses);

                        break;
                    }

                    buf = (ByteBuffer)req.message();
                }

                if (!skipWrite) {
                    Span span = tracing.create(COMMUNICATION_SOCKET_WRITE, req.span());

                    try (TraceSurroundings ignore = span.equals(NoopSpan.INSTANCE) ? null : MTC.support(span)) {
                        int cnt = sockCh.write(buf);

                        if (log.isTraceEnabled())
                            log.trace("Bytes sent [sockCh=" + sockCh + ", cnt=" + cnt + ']');

                        if (sentBytesCntMetric != null)
                            sentBytesCntMetric.add(cnt);

                        ses.bytesSent(cnt);
                    }
                }
                else {
                    // For test purposes only (skipWrite is set to true in tests only).
                    try {
                        U.sleep(50);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        throw new IOException("Thread has been interrupted.", e);
                    }
                }

                if (buf.remaining() > 0) {
                    // Not all data was written.
                    ses.addMeta(BUF_META_KEY, buf);
                    ses.addMeta(NIO_OPERATION.ordinal(), req);

                    break;
                }
                else {
                    // Message was successfully written.
                    assert req != null;

                    try {
                        req.onMessageWritten();
                        filterChain.onMessageSent(ses, req.message());
                    }
                    catch (IgniteCheckedException e) {
                        close(ses, e);
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ByteBufferNioClientWorker.class, this, super.toString());
        }
    }

    /**
     * A separate thread that will accept incoming connections and schedule read to some worker.
     */
    private class GridNioAcceptWorker extends GridWorker {
        /** Selector for this thread. */
        private Selector selector;

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param name Thread name.
         * @param log Log.
         * @param selector Which will accept incoming connections.
         * @param workerLsnr Worker lifecycle listener.
         */
        protected GridNioAcceptWorker(
            @Nullable String igniteInstanceName,
            String name,
            IgniteLogger log,
            Selector selector,
            @Nullable GridWorkerListener workerLsnr
        ) {
            super(igniteInstanceName, name, log, workerLsnr);

            this.selector = selector;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            super.cancel();

            // If accept worker never was started then explicitly close selector, otherwise selector will be closed
            // in finally block when workers thread will be stopped.
            if (runner() == null)
                closeSelector();
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                boolean reset = false;

                while (!closed && !isCancelled()) {
                    try {
                        if (reset)
                            selector = createSelector(locAddr);

                        accept();
                    }
                    catch (IgniteCheckedException e) {
                        if (!Thread.currentThread().isInterrupted()) {
                            U.error(log, "Failed to accept remote connection (will wait for " + ERR_WAIT_TIME + "ms).",
                                e);

                            U.sleep(ERR_WAIT_TIME);

                            reset = true;
                        }
                    }
                }
            }
            catch (Throwable t) {
                if (!(t instanceof IgniteInterruptedCheckedException))
                    err = t;

                throw t;
            }
            finally {
                try {
                    closeSelector(); // Safety.
                }
                catch (RuntimeException ignore) {
                    // No-op.
                }

                if (err == null && !closed)
                    err = new IllegalStateException("Thread " + name() + " is terminated unexpectedly");

                if (err instanceof OutOfMemoryError)
                    lsnr.onFailure(CRITICAL_ERROR, err);
                else if (err != null)
                    lsnr.onFailure(SYSTEM_WORKER_TERMINATION, err);
                else
                    // In case of closed == true, prevent general-case termination handling.
                    cancel();
            }
        }

        /**
         * Accepts connections and schedules them for processing by one of read workers.
         *
         * @throws IgniteCheckedException If failed.
         */
        private void accept() throws IgniteCheckedException {
            try {
                while (!closed && selector.isOpen() && !Thread.currentThread().isInterrupted()) {
                    updateHeartbeat();

                    // Wake up every 2 seconds to check if closed.
                    if (selector.select(2000) > 0)
                        // Walk through the ready keys collection and process date requests.
                        processSelectedKeys(selector.selectedKeys());
                    else
                        updateHeartbeat();

                    if (balancer != null)
                        balancer.run();

                    onIdle();
                }
            }
            // Ignore this exception as thread interruption is equal to 'close' call.
            catch (ClosedByInterruptException e) {
                if (log.isDebugEnabled())
                    log.debug("Closing selector due to thread interruption [srvr=" + this +
                        ", err=" + e.getMessage() + ']');
            }
            catch (ClosedSelectorException e) {
                throw new IgniteCheckedException("Selector got closed while active: " + this, e);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to accept connection: " + this, e);
            }
            finally {
                closeSelector();
            }
        }

        /**
         * Close selector if needed.
         */
        private void closeSelector() {
            if (selector.isOpen()) {
                if (log.isDebugEnabled())
                    log.debug("Closing all listening sockets.");

                // Close all channels registered with selector.
                for (SelectionKey key : selector.keys())
                    U.close(key.channel(), log);

                if (log.isDebugEnabled())
                    log.debug("Closing NIO selector.");

                U.close(selector, log);
            }
        }

        /**
         * Processes selected accept requests for server socket.
         *
         * @param keys Selected keys from acceptor.
         * @throws IOException If accept failed or IOException occurred while configuring channel.
         */
        private void processSelectedKeys(Set<SelectionKey> keys) throws IOException {
            if (log.isDebugEnabled())
                log.debug("Processing keys in accept worker: " + keys.size());

            for (Iterator<SelectionKey> iter = keys.iterator(); iter.hasNext(); ) {
                SelectionKey key = iter.next();

                iter.remove();

                // Was key closed?
                if (!key.isValid())
                    continue;

                if (key.isAcceptable()) {
                    // The key indexes into the selector so we
                    // can retrieve the socket that's ready for I/O
                    ServerSocketChannel srvrCh = (ServerSocketChannel)key.channel();

                    SocketChannel sockCh = srvrCh.accept();

                    sockCh.configureBlocking(false);
                    sockCh.socket().setTcpNoDelay(tcpNoDelay);
                    sockCh.socket().setKeepAlive(true);

                    if (sockSndBuf > 0)
                        sockCh.socket().setSendBufferSize(sockSndBuf);

                    if (sockRcvBuf > 0)
                        sockCh.socket().setReceiveBufferSize(sockRcvBuf);

                    if (log.isDebugEnabled())
                        log.debug("Accepted new client connection: " + sockCh.socket().getRemoteSocketAddress());

                    addRegistrationRequest(sockCh);
                }
            }
        }

        /**
         * Adds registration request for a given socket channel to the next selector. Next selector
         * is selected according to a round-robin algorithm.
         *
         * @param sockCh Socket channel to be registered on one of the selectors.
         */
        private void addRegistrationRequest(SocketChannel sockCh) {
            offer(new SessionRegisterFuture(NioOperation.REGISTER, sockCh, null, true));
        }
    }

    /**
     * Filter forwarding messages from chain's head to this server.
     */
    private class HeadFilter extends GridAbstractNioFilter {
        /**
         * Assigns filter name.
         */
        protected HeadFilter() {
            super("HeadFilter");
        }

        /** {@inheritDoc} */
        @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
            if (directMode && sslFilter != null)
                ses.addMeta(BUF_SSL_SYSTEM_META_KEY, new ConcurrentLinkedQueue<>());

            proceedSessionOpened(ses);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg, boolean createFuture,
            IgniteInClosure<IgniteException> ackC) throws IgniteCheckedException {
            assert ses instanceof GridSelectorNioSessionImpl;

            GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

            // ssl system message
            if (directMode && sslFilter != null && msg instanceof ByteBuffer) {
                ConcurrentLinkedQueue<ByteBuffer> queue = ses.meta(BUF_SSL_SYSTEM_META_KEY);

                assert queue != null;

                queue.offer((ByteBuffer)msg);

                if (impl.setWriting(true)) {
                    AbstractNioClientWorker worker = (AbstractNioClientWorker)impl.worker();

                    if (worker != null)
                        worker.startWriting(impl);
                }

                return null;
            }

            return send(ses, msg, createFuture, ackC);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses, IgniteCheckedException cause) {
            return close(ses, cause);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onPauseReads(GridNioSession ses) {
            return pauseReads(ses);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onResumeReads(GridNioSession ses) {
            return resumeReads(ses);
        }
    }

    /** */
    private class ReadWriteSizeBasedBalancer implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private long lastBalance;

        /** */
        private final long balancePeriod;

        /**
         * @param balancePeriod Period.
         */
        ReadWriteSizeBasedBalancer(long balancePeriod) {
            this.balancePeriod = balancePeriod;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            long now = U.currentTimeMillis();

            if (lastBalance + balancePeriod < now) {
                lastBalance = now;

                long maxRcvd0 = -1, minRcvd0 = -1, maxSent0 = -1, minSent0 = -1;
                int maxRcvdIdx = -1, minRcvdIdx = -1, maxSentIdx = -1, minSentIdx = -1;

                for (int i = 0; i < clientWorkers.size(); i++) {
                    AbstractNioClientWorker worker = clientWorkers.get(i);

                    int sesCnt = worker.workerSessions.size();

                    if (i % 2 == 0) {
                        // Reader.
                        long bytesRcvd0 = worker.bytesRcvd0;

                        if ((maxRcvd0 == -1 || bytesRcvd0 > maxRcvd0) && bytesRcvd0 > 0 && sesCnt > 1) {
                            maxRcvd0 = bytesRcvd0;
                            maxRcvdIdx = i;
                        }

                        if (minRcvd0 == -1 || bytesRcvd0 < minRcvd0) {
                            minRcvd0 = bytesRcvd0;
                            minRcvdIdx = i;
                        }
                    }
                    else {
                        // Writer.
                        long bytesSent0 = worker.bytesSent0;

                        if ((maxSent0 == -1 || bytesSent0 > maxSent0) && bytesSent0 > 0 && sesCnt > 1) {
                            maxSent0 = bytesSent0;
                            maxSentIdx = i;
                        }

                        if (minSent0 == -1 || bytesSent0 < minSent0) {
                            minSent0 = bytesSent0;
                            minSentIdx = i;
                        }
                    }
                }

                if (log.isDebugEnabled())
                    log.debug("Balancing data [minSent0=" + minSent0 + ", minSentIdx=" + minSentIdx +
                        ", maxSent0=" + maxSent0 + ", maxSentIdx=" + maxSentIdx +
                        ", minRcvd0=" + minRcvd0 + ", minRcvdIdx=" + minRcvdIdx +
                        ", maxRcvd0=" + maxRcvd0 + ", maxRcvdIdx=" + maxRcvdIdx + ']');

                if (maxSent0 != -1 && minSent0 != -1) {
                    GridSelectorNioSessionImpl ses = null;

                    long sentDiff = maxSent0 - minSent0;
                    long delta = sentDiff;
                    double threshold = sentDiff * 0.9;

                    GridConcurrentHashSet<GridSelectorNioSessionImpl> sessions =
                        clientWorkers.get(maxSentIdx).workerSessions;

                    for (GridSelectorNioSessionImpl ses0 : sessions) {
                        long bytesSent0 = ses0.bytesSent0();

                        if (bytesSent0 < threshold &&
                            (ses == null || delta > U.safeAbs(bytesSent0 - sentDiff / 2))) {
                            ses = ses0;
                            delta = U.safeAbs(bytesSent0 - sentDiff / 2);
                        }
                    }

                    if (ses != null) {
                        if (log.isDebugEnabled())
                            log.debug("Will move session to less loaded writer [ses=" + ses +
                                ", from=" + maxSentIdx + ", to=" + minSentIdx + ']');

                        move(ses, maxSentIdx, minSentIdx);
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Unable to find session to move for writers.");
                    }
                }

                if (maxRcvd0 != -1 && minRcvd0 != -1) {
                    GridSelectorNioSessionImpl ses = null;

                    long rcvdDiff = maxRcvd0 - minRcvd0;
                    long delta = rcvdDiff;
                    double threshold = rcvdDiff * 0.9;

                    GridConcurrentHashSet<GridSelectorNioSessionImpl> sessions =
                        clientWorkers.get(maxRcvdIdx).workerSessions;

                    for (GridSelectorNioSessionImpl ses0 : sessions) {
                        long bytesRcvd0 = ses0.bytesReceived0();

                        if (bytesRcvd0 < threshold &&
                            (ses == null || delta > U.safeAbs(bytesRcvd0 - rcvdDiff / 2))) {
                            ses = ses0;
                            delta = U.safeAbs(bytesRcvd0 - rcvdDiff / 2);
                        }
                    }

                    if (ses != null) {
                        if (log.isDebugEnabled())
                            log.debug("Will move session to less loaded reader [ses=" + ses +
                                ", from=" + maxRcvdIdx + ", to=" + minRcvdIdx + ']');

                        move(ses, maxRcvdIdx, minRcvdIdx);
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Unable to find session to move for readers.");
                    }
                }

                for (int i = 0; i < clientWorkers.size(); i++) {
                    AbstractNioClientWorker worker = clientWorkers.get(i);

                    worker.reset0();
                }
            }
        }
    }

    /** */
    private class SizeBasedBalancer implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private long lastBalance;

        /** */
        private final long balancePeriod;

        /**
         * @param balancePeriod Period.
         */
        SizeBasedBalancer(long balancePeriod) {
            this.balancePeriod = balancePeriod;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            long now = U.currentTimeMillis();

            if (lastBalance + balancePeriod < now) {
                lastBalance = now;

                long maxBytes0 = -1, minBytes0 = -1;
                int maxBytesIdx = -1, minBytesIdx = -1;

                for (int i = 0; i < clientWorkers.size(); i++) {
                    AbstractNioClientWorker worker = clientWorkers.get(i);

                    int sesCnt = worker.workerSessions.size();

                    long bytes0 = worker.bytesRcvd0 + worker.bytesSent0;

                    if ((maxBytes0 == -1 || bytes0 > maxBytes0) && bytes0 > 0 && sesCnt > 1) {
                        maxBytes0 = bytes0;
                        maxBytesIdx = i;
                    }

                    if (minBytes0 == -1 || bytes0 < minBytes0) {
                        minBytes0 = bytes0;
                        minBytesIdx = i;
                    }
                }

                if (log.isDebugEnabled())
                    log.debug("Balancing data [min0=" + minBytes0 + ", minIdx=" + minBytesIdx +
                        ", max0=" + maxBytes0 + ", maxIdx=" + maxBytesIdx + ']');

                if (maxBytes0 != -1 && minBytes0 != -1) {
                    GridSelectorNioSessionImpl ses = null;

                    long bytesDiff = maxBytes0 - minBytes0;
                    long delta = bytesDiff;
                    double threshold = bytesDiff * 0.9;

                    GridConcurrentHashSet<GridSelectorNioSessionImpl> sessions =
                        clientWorkers.get(maxBytesIdx).workerSessions;

                    for (GridSelectorNioSessionImpl ses0 : sessions) {
                        long bytesSent0 = ses0.bytesSent0();

                        if (bytesSent0 < threshold &&
                            (ses == null || delta > U.safeAbs(bytesSent0 - bytesDiff / 2))) {
                            ses = ses0;
                            delta = U.safeAbs(bytesSent0 - bytesDiff / 2);
                        }
                    }

                    if (ses != null) {
                        if (log.isDebugEnabled())
                            log.debug("Will move session to less loaded worker [ses=" + ses +
                                ", from=" + maxBytesIdx + ", to=" + minBytesIdx + ']');

                        move(ses, maxBytesIdx, minBytesIdx);
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Unable to find session to move.");
                    }
                }

                for (int i = 0; i < clientWorkers.size(); i++) {
                    AbstractNioClientWorker worker = clientWorkers.get(i);

                    worker.reset0();
                }
            }
        }
    }

    /**
     * For tests only.
     */
    private class RandomBalancer implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void run() {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            int w1 = rnd.nextInt(clientWorkers.size());

            if (clientWorkers.get(w1).workerSessions.isEmpty())
                return;

            int w2 = rnd.nextInt(clientWorkers.size());

            while (w2 == w1)
                w2 = rnd.nextInt(clientWorkers.size());

            GridSelectorNioSessionImpl ses = randomSession(clientWorkers.get(w1));

            if (ses != null) {
                if (log.isInfoEnabled())
                    log.info("Move session [from=" + w1 +
                        ", to=" + w2 +
                        ", ses=" + ses + ']');

                move(ses, w1, w2);
            }
        }

        /**
         * @param worker Worker.
         * @return NIO session.
         */
        private GridSelectorNioSessionImpl randomSession(AbstractNioClientWorker worker) {
            Collection<GridSelectorNioSessionImpl> sessions = worker.workerSessions;

            int size = sessions.size();

            if (size == 0)
                return null;

            int idx = ThreadLocalRandom.current().nextInt(size);

            Iterator<GridSelectorNioSessionImpl> it = sessions.iterator();

            int cnt = 0;

            while (it.hasNext()) {
                GridSelectorNioSessionImpl ses = it.next();

                if (cnt == idx)
                    return ses;
            }

            return null;
        }
    }
}

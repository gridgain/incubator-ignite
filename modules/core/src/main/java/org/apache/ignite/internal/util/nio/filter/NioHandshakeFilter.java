package org.apache.ignite.internal.util.nio.filter;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.util.nio.ClusterDiscovery;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.nio.GridNioWorker;
import org.apache.ignite.internal.util.nio.GridSelectorNioSessionImpl;
import org.apache.ignite.internal.util.nio.operation.ConnectOperationRequest;
import org.apache.ignite.internal.util.nio.operation.SessionOperation;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.spi.communication.tcp.internal.CommunicationClientRegistry;
import org.apache.ignite.spi.communication.tcp.internal.ConnectGateway;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;
import org.apache.ignite.spi.communication.tcp.internal.HandshakeException;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage2;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;
import org.apache.ignite.spi.communication.tcp.messages.IgniteHeaderMessage;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessage;
import org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage;
import org.apache.ignite.spi.communication.tcp.messages.SystemMessage;
import org.apache.ignite.spi.communication.tcp.messages.SystemMessageFactoryProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteFeatures.TCP_COMMUNICATION_SPI_HANDSHAKE_WAIT_MESSAGE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.CONN_IDX_META;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.CONSISTENT_ID_META;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.makeMessageType;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.ALREADY_CONNECTED;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.NEED_WAIT;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.NODE_STOPPING;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.UNKNOWN_NODE;

public class NioHandshakeFilter extends GridAbstractNioFilter {
    /** */
    private static final int CONTEXT_META = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    private final ClusterDiscovery discovery;

    /** */
    private final CommunicationClientRegistry clientRegistry;

    /** */
    private final CountDownLatch initLatch;

    /** */
    private final ConnectGateway gateway;

    /** */
    private final boolean client;

    /** */
    public NioHandshakeFilter(ClusterDiscovery discovery,
        CommunicationClientRegistry clientRegistry, CountDownLatch initLatch,
        ConnectGateway gateway, boolean client) {
        this.discovery = discovery;
        this.clientRegistry = clientRegistry;
        this.initLatch = initLatch;
        this.gateway = gateway;
        this.client = client;
    }

    private enum State {
        NEW,
        RESERVING_RECOVERY_DESCRIPTOR,
        RESERVED_RECOVERY_DESCRIPTOR,
        NODE_ID_MSG_SENT,
        HANDSHAKE_MSG_SENT,
        HANDSHAKE_DONE,
        HANDSHAKE_FAILED
    }

    private static interface MessageHolder {
        SystemMessage readMessage(GridNioSession ses, ByteBuffer buf) throws IgniteCheckedException;
    }

    private static class HeaderMessageHolder implements MessageHolder {
        private final byte[] magicBuf = new byte[U.IGNITE_HEADER.length];
        private int read;

        @Override public SystemMessage readMessage(GridNioSession ses, ByteBuffer buf) throws IgniteCheckedException {
            if (read < U.IGNITE_HEADER.length) {
                int cnt = Math.min(U.IGNITE_HEADER.length - read, buf.remaining());
                buf.get(magicBuf, read, cnt);
                read += cnt;
            }

            if (read == U.IGNITE_HEADER.length) {
                if (U.bytesEqual(magicBuf, 0, U.IGNITE_HEADER, 0, U.IGNITE_HEADER.length))
                    return IgniteHeaderMessage.INSTANCE;
                else
                    throw new IgniteCheckedException("Unknown connection detected (is some other software connecting to this " +
                        "Ignite port?) [rmtAddr=" + ses.remoteAddress() + ", locAddr=" + ses.localAddress() + ']');
            }

            return null;
        }
    }

    private static class SystemMessageHolder implements MessageHolder {
        /** */
        @SuppressWarnings("deprecation")
        private static final IgniteMessageFactory FACTORY =
            new IgniteMessageFactoryImpl(new MessageFactory[] { new SystemMessageFactoryProvider() });

        /** */
        private SystemMessage msg;

        /** */
        private boolean finished;

        /** */
        @Override public SystemMessage readMessage(GridNioSession ses, ByteBuffer buf) throws IgniteCheckedException {
            try {
                if (msg == null) {
                    if (buf.remaining() < Message.DIRECT_TYPE_SIZE)
                        return null;

                    msg = (SystemMessage)FACTORY.create(makeMessageType(buf.get(), buf.get()));
                }

                if (finished || (finished = msg.readFrom(buf)))
                    return msg;

                return null;
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Failed to read system message.", e);
            }
        }
    }

    private static class HandshakeContext {
        private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
        private final UUID localNodeId;

        private ConnectionKey connKey;
        private GridCommunicationClient client;
        private long connCntr = -1;

        private boolean connectionBytesVerified;
        private boolean remoteNodeIdVerified;

        /** */
        private MessageHolder messageHolder;

        private HandshakeContext(UUID localNodeId, @Nullable ConnectionKey connKey, @Nullable GridCommunicationClient client) {
            this.localNodeId = localNodeId;
            this.connKey = connKey;
            this.client = client;
        }

        public UUID localNodeId() {
            return localNodeId;
        }

        public UUID remoteNodeId() {
            return connectionKey() == null ? null : connectionKey().nodeId();
        }

        public ConnectionKey connectionKey() {
            return connKey;
        }

        public void connectionKey(ConnectionKey connKey) {
            this.connKey = connKey;
        }

        public int connectionIndex() {
            return client() == null ? -1 : client().connectionIndex();
        }

        public long connectionCounter() {
            return connCntr;
        }

        void incrementConnectionCounter() {
            assert connectionCounter() == -1;

            connCntr = recovery().incrementConnectCount();
        }

        public GridCommunicationClient client() {
            return client;
        }

        public void client(GridCommunicationClient client) {
            this.client = client;
        }

        public GridNioRecoveryDescriptor recovery() {
            if (client() == null)
                return null;

            GridNioRecoveryDescriptor recovery = client().recovery();

            assert recovery != null;

            return recovery;
        }

        State state() {
            return state.get();
        }

        boolean moveState(State from, State to) {
            return state.get() == from && state.compareAndSet(from, to);
        }

        public SystemMessage readMessage(GridNioSession ses, ByteBuffer buf) throws IgniteCheckedException {
            assert messageHolder == null || messageHolder instanceof SystemMessageHolder;
            if (messageHolder == null)
                messageHolder = new SystemMessageHolder();

            SystemMessage msg = messageHolder.readMessage(ses, buf);

            if (msg != null)
                messageHolder = null;

            return msg;
        }

        public boolean connectionBytesVerified() {
            return connectionBytesVerified;
        }

        public boolean verifyCinnectionBytes(GridNioSession ses, ByteBuffer buf) throws IgniteCheckedException {
            if (connectionBytesVerified())
                return true;

            assert messageHolder == null || messageHolder instanceof HeaderMessageHolder;
            if (messageHolder == null)
                messageHolder = new HeaderMessageHolder();

            SystemMessage msg = messageHolder.readMessage(ses, buf);

            if (msg == null)
                return false;

            assert msg == IgniteHeaderMessage.INSTANCE;

            connectionBytesVerified = true;
            messageHolder = null;

            return true;
        }

        public boolean remoteNodeIdVerified() {
            return remoteNodeIdVerified;
        }

        public void verifyRemoteNodeId(byte[] remoteNodeIdBytes) throws IgniteCheckedException {
            if (Objects.equals(U.bytesToUuid(remoteNodeIdBytes, 0), remoteNodeId()))
                remoteNodeIdVerified = true;
            else
                throw new IgniteCheckedException("Unexpected node id. nodeId=" + U.bytesToUuid(remoteNodeIdBytes, 0));
        }
    }

    /** */
    @Override public void onSessionOpened(GridNioSession ses) {
        assert ses instanceof GridSelectorNioSessionImpl;

        startHandshake(createContext(ses), (GridSelectorNioSessionImpl)ses);
    }

    /** */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        assert ses instanceof GridSelectorNioSessionImpl && msg instanceof ByteBuffer;

        HandshakeContext ctx = ses.meta(CONTEXT_META);

        assert ctx != null;

        if (ctx.state() != State.HANDSHAKE_DONE)
            proceedHandshake(ctx, (GridSelectorNioSessionImpl)ses, (ByteBuffer)msg);
        if (ctx.state() == State.HANDSHAKE_DONE)
            proceedMessageReceived(ses, msg);
    }

    /** */
    private void startHandshake(HandshakeContext ctx, GridSelectorNioSessionImpl ses) {
        try {
            gateway.enter();

            if (ses.accepted()) { // incomming connection
                if (needWait() && ctx.moveState(State.NEW, State.HANDSHAKE_FAILED))
                    ses.send(new HandshakeWaitMessage()).listen(fut -> ses.close());
                else if (ctx.moveState(State.NEW, State.NODE_ID_MSG_SENT))
                    ses.sendNoFuture(new NodeIdMessage(ctx.localNodeId()));
                else
                    throw unexpectedStateException(ctx.state());
            }
            else { // outgoing connection
                if (!ctx.moveState(State.NEW, State.RESERVING_RECOVERY_DESCRIPTOR))
                    throw unexpectedStateException(ctx.state());

                GridCommunicationClient client = ctx.client();

                assert client != null;

                ctx.incrementConnectionCounter();

                IgniteInClosure<Boolean> clo = reserved -> {
                    try {
                        if (!reserved) {
                            if (!ctx.moveState(State.RESERVING_RECOVERY_DESCRIPTOR, State.HANDSHAKE_FAILED))
                                throw unexpectedStateException(ctx.state());
                            else
                                ses.close();

                            return;
                        }

                        if (!ctx.moveState(State.RESERVING_RECOVERY_DESCRIPTOR, State.RESERVED_RECOVERY_DESCRIPTOR)) {
                            ctx.recovery().release();

                            throw unexpectedStateException(ctx.state());
                        }

                        if (ctx.remoteNodeIdVerified() && ctx.moveState(State.RESERVED_RECOVERY_DESCRIPTOR, State.HANDSHAKE_MSG_SENT)) {
                            ses.sendNoFuture(IgniteHeaderMessage.INSTANCE);
                            ses.sendNoFuture(new HandshakeMessage2(ctx.localNodeId(), ctx.connectionCounter(), ctx.recovery().received(), ctx.connectionIndex()));
                        }
                    }
                    catch (IgniteCheckedException e) {
                        hanshakeFailed(ctx, ses, e);
                    }
                };

                if (ctx.recovery().tryReserve(ctx.connectionCounter(), clo))
                    clo.apply(true);
            }
        }
        catch (IgniteCheckedException e) {
            hanshakeFailed(ctx, ses, e);
        }
    }

    private void proceedHandshake(HandshakeContext ctx, GridSelectorNioSessionImpl ses, ByteBuffer buff) {
        try {
            if (ses.accepted()) { // Incomming connection
                if (!ctx.connectionBytesVerified() && !ctx.verifyCinnectionBytes(ses, buff))
                    return;

                SystemMessage msg = ctx.readMessage(ses, buff);
                if (msg == null) // incomplete
                    return;

                if (!gateway.tryEnter()) {
                    if (!ctx.moveState(State.NODE_ID_MSG_SENT, State.HANDSHAKE_FAILED))
                        throw unexpectedStateException(ctx.state());

                    ses.send(new RecoveryLastReceivedMessage(NODE_STOPPING)).listen(fut -> ses.close());

                    return;
                }

                if (ctx.state() != State.NODE_ID_MSG_SENT)
                    throw unexpectedStateException(ctx.state());
                else {
                    if (msg instanceof HandshakeMessage2) {
                        HandshakeMessage2 msg0 = (HandshakeMessage2)msg;

                        if (!discovery.known(msg0.nodeId())) {
                            if (!ctx.moveState(State.NODE_ID_MSG_SENT, State.HANDSHAKE_FAILED))
                                throw unexpectedStateException(ctx.state());

                            ses.send(new RecoveryLastReceivedMessage(UNKNOWN_NODE)).listen(fut -> ses.close());

                            return;
                        }

                        ClusterNode node = discovery.remoteNode(msg0.nodeId());

                        if (node == null) {
                            if (!ctx.moveState(State.NODE_ID_MSG_SENT, State.HANDSHAKE_FAILED))
                                throw unexpectedStateException(ctx.state());

                            ses.send(new RecoveryLastReceivedMessage(NEED_WAIT)).listen(fut -> ses.close());

                            return;
                        }

                        ConnectionKey connKey = ConnectionKey.newIncomingKey(msg0.nodeId(), msg0.connectionIndex(), msg0.connectCount());
                        ctx.connectionKey(connKey);
                        ctx.client(clientRegistry.client(connKey));

                        if (!ctx.moveState(State.NODE_ID_MSG_SENT, State.RESERVING_RECOVERY_DESCRIPTOR))
                            throw unexpectedStateException(ctx.state());

                        IgniteInClosure<Boolean> clo = reserved -> {
                            try {
                                if (!reserved) {
                                    if (!ctx.moveState(State.NODE_ID_MSG_SENT, State.HANDSHAKE_FAILED))
                                        throw unexpectedStateException(ctx.state());

                                    ses.send(new RecoveryLastReceivedMessage(ALREADY_CONNECTED)).listen(fut -> ses.close());

                                    return;
                                }

                                if (!ctx.moveState(State.RESERVING_RECOVERY_DESCRIPTOR, State.RESERVED_RECOVERY_DESCRIPTOR)) {
                                    ctx.recovery().release();

                                    throw unexpectedStateException(ctx.state());
                                }

                                if (!ctx.moveState(State.RESERVED_RECOVERY_DESCRIPTOR, State.HANDSHAKE_DONE))
                                    throw unexpectedStateException(ctx.state());

                                ses.offerStateChange(new SessionOperation(ses) {
                                    @Override public void run() {
                                        GridNioRecoveryDescriptor recovery = ctx.recovery();

                                        session().addMeta(CONN_IDX_META, ctx.connectionKey());
                                        session().addMeta(CONSISTENT_ID_META, node.consistentId());
                                        session().addMeta(GridNioServer.RECOVERY_DESC_META_KEY, recovery);

                                        recovery.onHandshake(msg0.received());

                                        ses.inRecoveryDescriptor(recovery);
                                        if (!recovery.pairedConnections())
                                            ses.outRecoveryDescriptor(recovery);

                                        recovery.onConnected();

                                        gateway.leave();

                                        doProceedSessionOpened(ses);
                                    }
                                });

                                ses.sendNoFuture(new RecoveryLastReceivedMessage(ctx.recovery().received()));
                            }
                            catch (IgniteCheckedException e) {
                                hanshakeFailed(ctx, ses, e);
                            }
                        };

                        if (ctx.recovery().tryReserve(msg0.connectCount(), clo))
                            clo.apply(true);
                    }
                    else
                        throw new IgniteCheckedException("Unexpected message type. msg=" + msg);
                }
            }
            else { // Outgoing connection
                SystemMessage msg0 = ctx.readMessage(ses, buff);
                if (msg0 == null) // incomplete
                    return;

                switch (ctx.state()) {
                    case NEW:
                    case RESERVING_RECOVERY_DESCRIPTOR:
                    case RESERVED_RECOVERY_DESCRIPTOR: {
                        if (msg0 instanceof HandshakeWaitMessage)
                            throw new HandshakeException("Need wait.");

                        if (msg0 instanceof NodeIdMessage)
                            ctx.verifyRemoteNodeId(((NodeIdMessage)msg0).nodeIdBytes());
                        else
                            throw new IgniteCheckedException("Unexpected message type. msg=" + msg0);

                        if (ctx.moveState(State.RESERVED_RECOVERY_DESCRIPTOR, State.HANDSHAKE_MSG_SENT)) {
                            ses.sendNoFuture(IgniteHeaderMessage.INSTANCE);
                            ses.sendNoFuture(new HandshakeMessage2(ctx.localNodeId(), ctx.connectionCounter(), ctx.recovery().received(), ctx.connectionIndex()));
                        }

                        break;
                    }

                    case HANDSHAKE_MSG_SENT: {
                        if (msg0 instanceof RecoveryLastReceivedMessage) {
                            long received = ((RecoveryLastReceivedMessage)msg0).received();
                            if (received == ALREADY_CONNECTED)
                                throw new IgniteCheckedException("Already connected.");
                            else if (received == NODE_STOPPING)
                                throw new ClusterTopologyCheckedException("Remote node started stop procedure: " + ctx.remoteNodeId());
                            else if (received == UNKNOWN_NODE)
                                throw new IgniteCheckedException("Remote node does not observe current node in topology : " + ctx.remoteNodeId());
                            else if (received == NEED_WAIT)
                                throw new HandshakeException("Need wait.");
                            else {
                                ClusterNode node = discovery.remoteNode(ctx.remoteNodeId());
                                if (node == null)
                                    throw new ClusterTopologyCheckedException("Remote left topology: " + ctx.remoteNodeId());
                                if (ctx.moveState(State.HANDSHAKE_MSG_SENT, State.HANDSHAKE_DONE)){
                                    GridNioRecoveryDescriptor recovery = ctx.recovery();

                                    ses.addMeta(CONSISTENT_ID_META, node.consistentId());
                                    ses.addMeta(GridNioServer.RECOVERY_DESC_META_KEY, recovery);

                                    recovery.onHandshake(received);

                                    ses.outRecoveryDescriptor(recovery);
                                    if (!recovery.pairedConnections())
                                        ses.inRecoveryDescriptor(recovery);

                                    recovery.onConnected();

                                    gateway.leave();

                                    doProceedSessionOpened(ses);
                                }
                            }
                        }
                        else
                            throw new IgniteCheckedException("Unexpected message type. msg=" + msg0);
                    }
                    default:
                        throw unexpectedStateException(ctx.state());
                }
            }
        }
        catch (IgniteCheckedException e) {
            hanshakeFailed(ctx, ses, e);
        }
    }

    /** */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses,
        @Nullable IgniteCheckedException cause) throws IgniteCheckedException {
        if (cause instanceof HandshakeException) {
            // TODO do better
            GridSelectorNioSessionImpl ses0 = (GridSelectorNioSessionImpl)ses;
            GridNioWorker worker = ses0.worker();

            assert worker != null;

            new Thread(() -> {
                try {
                    Thread.sleep(200);

                    worker.offer(new ConnectOperationRequest());
                }
                catch (InterruptedException e) {
                    return;
                }


            }).start();
        }

        return super.onSessionClose(ses, cause);
    }

    /** */
    private void doProceedSessionOpened(GridSelectorNioSessionImpl ses) {
        try {
            proceedSessionOpened(ses);
        }
        catch (IgniteCheckedException e) {
            ses.close(e);
        }
    }

    /** */
    private void hanshakeFailed(HandshakeContext ctx, GridSelectorNioSessionImpl ses, IgniteCheckedException cause) {
        while (true) {
            final State prevState = ctx.state();

            if (prevState == State.HANDSHAKE_FAILED)
                break;
            if (!ctx.moveState(prevState, State.HANDSHAKE_FAILED))
                continue;
            if (releaseRecoveryDescriptor(prevState, ses))
                ctx.recovery().release();

            ses.close(cause);

            break;
        }

        gateway.leave();
    }

    /** */
    private boolean releaseRecoveryDescriptor(State state, GridSelectorNioSessionImpl ses) {
        assert state != State.HANDSHAKE_DONE && state != State.HANDSHAKE_FAILED : state;

        if (ses.accepted())
            return state == State.RESERVED_RECOVERY_DESCRIPTOR;
        else
            return state != State.NEW && state != State.RESERVING_RECOVERY_DESCRIPTOR;
    }

    private boolean needWait() {
        return !client && initLatch.getCount() != 0 && discovery.allNodesSupport(TCP_COMMUNICATION_SPI_HANDSHAKE_WAIT_MESSAGE);
    }

    @NotNull private HandshakeContext createContext(GridNioSession ses) {
        ConnectionKey connKey = ses.meta(CONN_IDX_META);
        HandshakeContext ctx = new HandshakeContext(discovery.localNodeId(), connKey, clientRegistry.client(connKey));
        Object prev = ses.addMeta(CONTEXT_META, ctx);
        assert prev == null;
        return ctx;
    }

    @NotNull private static IgniteCheckedException unexpectedStateException(State state) {
        return new IgniteCheckedException("Unexpected state: " + state);
    }
}

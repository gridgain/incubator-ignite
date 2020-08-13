package org.apache.ignite.internal.util.nio.filter;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.util.nio.ClusterDiscovery;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.nio.GridSelectorNioSessionImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.spi.communication.tcp.internal.CommunicationClientRegistry;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage2;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;
import org.apache.ignite.spi.communication.tcp.messages.IgniteHeaderMessage;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessage;
import org.apache.ignite.spi.communication.tcp.messages.SystemMessage;
import org.apache.ignite.spi.communication.tcp.messages.SystemMessageFactoryProvider;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.IgniteFeatures.TCP_COMMUNICATION_SPI_HANDSHAKE_WAIT_MESSAGE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.CONN_IDX_META;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.makeMessageType;

public class NioHandshakeFilter extends GridAbstractNioFilter {
    /** */
    private static final int CONTEXT_META = GridNioSessionMetaKey.nextUniqueKey();

    /** Default initial delay in case of target node is still out of topology. */
    private static final int DFLT_NEED_WAIT_DELAY = 200;

    /** */
    private static final UUID NEED_WAIT = UUID.randomUUID();

    private CommunicationClientRegistry clientRegistry;

    private ClusterDiscovery discovery;

    boolean client;

    CountDownLatch initLatch;

    private enum State {
        NEW,
        WAIT_FOR_HANDSHAKE,
        RESERVING_RECOVERY_DESCRIPTOR,
        RESERVED_RECOVERY_DESCRIPTOR,
        NODE_ID_CHECKED,
        NODE_ID_MSG_SENT,
        HANDSHAKE_MSG_SENT,
        HANDSHAKE_WAIT_MSG_SENT,
        HANDSHAKE_DONE,
        HANDSHAKE_FAILED
    }

    private static class IncompliteMessage {
        /** */
        private static final IgniteMessageFactory FACTORY = new IgniteMessageFactoryImpl(new MessageFactory[] { new SystemMessageFactoryProvider() });

        /** */
        private SystemMessage msg;

        /** */
        private boolean finished;

        public SystemMessage message() {
            return msg;
        }

        public boolean finished() {
            return finished;
        }

        /** */
        private boolean read(ByteBuffer buff) throws IgniteCheckedException {
            try {
                if (finished)
                    return true;

                if (msg == null) {
                    if (buff.remaining() < Message.DIRECT_TYPE_SIZE)
                        return false;
                    else
                        msg = (SystemMessage)FACTORY.create(makeMessageType(buff.get(), buff.get()));
                }

                assert msg != null;

                return finished = msg.readFrom(buff);
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Failed to read system message.", e);
            }
        }
    }

    private static class HandshakeContext {
        private final UUID localNodeId;
        private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
        private ConnectionKey connKey;
        private GridCommunicationClient client;
        private long connCntr = -1;
        private volatile UUID remoteNodeId;

        /** */
        private IncompliteMessage messageHolder = new IncompliteMessage();

        private HandshakeContext(UUID localNodeId, ConnectionKey connKey) {
            this.localNodeId = localNodeId;
            this.connKey = connKey;
        }

        public UUID localNodeId() {
            return localNodeId;
        }

        public UUID remoteNodeId() {
            return remoteNodeId;
        }

        public void remoteNodeId(UUID remoteNodeId) {
            this.remoteNodeId = remoteNodeId;
        }

        State state() {
            return state.get();
        }

        boolean moveState(State from, State to) {
            return state.get() == from && state.compareAndSet(from, to);
        }

        public GridCommunicationClient client() {
            return client;
        }

        public void client(GridCommunicationClient client) {
            this.client = client;
        }

        public ConnectionKey connKey() {
            return connKey;
        }

        public void connKey(ConnectionKey connKey) {
            this.connKey = connKey;
        }

        long connectionCounter() {
            return connCntr;
        }

        void obtainConnectionCounter() {
            assert connCntr == -1;

            connCntr = recovery().incrementConnectCount();
        }

        public int connectionIndex() {
            return client == null ? -1 : client.connectionIndex();
        }

        public GridNioRecoveryDescriptor recovery() {
            if (client == null)
                return null;

            GridNioRecoveryDescriptor recovery = client.recovery();

            assert recovery != null;

            return recovery;
        }

        public SystemMessage message() {
            if (!messageHolder.finished)
                return null;

            SystemMessage message = messageHolder.msg;

            messageHolder = new IncompliteMessage();

            return message;
        }

        public boolean readMessage(ByteBuffer buff) throws IgniteCheckedException {
            return messageHolder.read(buff);
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
            if (ses.accepted()) { // incomming connection
                if (needWait()) {
                    if (ctx.moveState(State.NEW, State.HANDSHAKE_WAIT_MSG_SENT))
                        ses.sendNoFuture(new HandshakeWaitMessage());
                    else
                        throw unexpectedStateException(ctx.state());
                }
                else if (ctx.moveState(State.NEW, State.NODE_ID_MSG_SENT))
                    ses.sendNoFuture(new NodeIdMessage(ctx.localNodeId()));
                else
                    throw unexpectedStateException(ctx.state());
            }
            else { // outgoing connection
                if (!ctx.moveState(State.NEW, State.RESERVING_RECOVERY_DESCRIPTOR))
                    throw unexpectedStateException(ctx.state());

                ctx.client(client(ses));
                ctx.obtainConnectionCounter();

                IgniteInClosure<Boolean> clo = reserved -> {
                    if (!reserved) {
                        ses.close();
                        return;
                    }

                    if (!ctx.moveState(State.RESERVING_RECOVERY_DESCRIPTOR, State.RESERVED_RECOVERY_DESCRIPTOR)) {
                        ctx.recovery().release();
                        ses.close();
                        return;
                    }

                    try {
                        UUID remoteNodeId = ctx.remoteNodeId();

                        if (remoteNodeId == null)
                            return;

                        if (remoteNodeId == NEED_WAIT) {
                            if (ctx.moveState(State.RESERVED_RECOVERY_DESCRIPTOR, State.WAIT_FOR_HANDSHAKE))
                                ses.idleTimeout(DFLT_NEED_WAIT_DELAY);
                        }
                        else if (!Objects.equals(remoteNodeId, ctx.connKey().nodeId()))
                            throw new IgniteCheckedException("Unexpected node id. nodeId=" + remoteNodeId);
                        else if (ctx.moveState(State.RESERVED_RECOVERY_DESCRIPTOR, State.HANDSHAKE_MSG_SENT)) {
                            ses.sendNoFuture(IgniteHeaderMessage.INSTANCE);
                            ses.sendNoFuture(new HandshakeMessage2(ctx.localNodeId(), ctx.connectionCounter(), ctx.recovery().received(), ctx.connectionIndex()));
                        }
                    }
                    catch (IgniteCheckedException e) {
                        hanshakeFailed(ctx, ses, e);
                    }
                };

                if (client(ses).recovery().tryReserve(ctx.connectionCounter(), clo))
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

            }
            else { // Outgoing connection
                switch (ctx.state()) {
                    case RESERVING_RECOVERY_DESCRIPTOR:
                    case RESERVED_RECOVERY_DESCRIPTOR: {
                        if (!ctx.readMessage(buff))
                            return;

                        // first expected message is a wait message or node id message
                        SystemMessage msg0 = ctx.message();
                        if (msg0 instanceof NodeIdMessage)
                            ctx.remoteNodeId(U.bytesToUuid(((NodeIdMessage)msg0).nodeIdBytes(), 0));
                        else if (msg0 instanceof HandshakeWaitMessage)
                            ctx.remoteNodeId(NEED_WAIT);
                        else
                            throw new IgniteCheckedException("Unexpected message type. msg=" + msg0);

                        UUID remoteNodeId = ctx.remoteNodeId();
                        assert remoteNodeId != null;

                        if (remoteNodeId == NEED_WAIT) {
                            if (ctx.moveState(State.RESERVED_RECOVERY_DESCRIPTOR, State.WAIT_FOR_HANDSHAKE))
                                ses.idleTimeout(DFLT_NEED_WAIT_DELAY);
                        }
                        else if (!Objects.equals(remoteNodeId, ctx.connKey().nodeId()))
                            throw new IgniteCheckedException("Unexpected node id. nodeId=" + remoteNodeId);
                        else if (ctx.moveState(State.RESERVED_RECOVERY_DESCRIPTOR, State.HANDSHAKE_MSG_SENT)) {
                            ses.sendNoFuture(IgniteHeaderMessage.INSTANCE);
                            ses.sendNoFuture(new HandshakeMessage2(ctx.localNodeId(), ctx.connectionCounter(), ctx.recovery().received(), ctx.connectionIndex()));
                        }

                        break;
                    }

                    case HANDSHAKE_MSG_SENT: {
                        if (!ctx.readMessage(buff))
                            return;

                        // here we expect

                    }
                    case WAIT_FOR_HANDSHAKE:

                    default:
                        unexpectedState(ctx, ses);
                }
            }

            switch (ctx.state()) {
                case WAIT_FOR_HANDSHAKE: {
                    assert !ses.accepted();

                    ses.sendNoFuture(IgniteHeaderMessage.INSTANCE);
                    ses.sendNoFuture(new HandshakeMessage2(localNodeId, connCntr, recovery.received(), connIdx));

                    if (!ctx.moveState(State.WAIT_FOR_HANDSHAKE, State.HANDSHAKE_MSG_SENT))
                        unexpectedState(ctx, ses);

                    break;
                }

                case HANDSHAKE_MSG_SENT:
                    processHandshakeResponse(ctx, ses, msg);

                    break;

                default:
                    throw unexpectedStateException(ctx.state());
            }
        }
        catch (IgniteCheckedException e) {
            hanshakeFailed(ctx, ses, e);
        }
    }

    /** */
    private void unexpectedState(HandshakeContext ctx, GridSelectorNioSessionImpl ses) {
        hanshakeFailed(ctx, ses, unexpectedStateException(ctx.state()));
    }

    /** */
    private void hanshakeFailed(HandshakeContext ctx, GridSelectorNioSessionImpl ses, IgniteCheckedException cause) {
        while (true) {
            State state = ctx.state();
            if (state == State.HANDSHAKE_FAILED)
                return;
            if (!ctx.moveState(state, State.HANDSHAKE_FAILED))
                continue;
            if (releaseRecoveryDescriptor(state, ses))
                ctx.recovery().release();

            ses.close(cause);

            return;
        }
    }

    /** */
    private boolean releaseRecoveryDescriptor(State state, GridSelectorNioSessionImpl ses) {
        return true;
    }

    /** */
    private GridCommunicationClient client(GridSelectorNioSessionImpl ses) {
        ConnectionKey connKey = connectionKey(ses);
        return connKey == null ? null : clientRegistry.client(connKey);
    }

    /** */
    private ConnectionKey connectionKey(GridSelectorNioSessionImpl ses) {
        return ses.meta(CONN_IDX_META);
    }

    @NotNull private static IgniteCheckedException unexpectedStateException(State state) {
        return new IgniteCheckedException("Unexpected state: " + state);
    }

    boolean needWait() {
        return !client && initLatch.getCount() != 0 && discovery.allNodesSupport(TCP_COMMUNICATION_SPI_HANDSHAKE_WAIT_MESSAGE);
    }

    @NotNull public NioHandshakeFilter.HandshakeContext createContext(GridNioSession ses) {
        HandshakeContext ctx = new HandshakeContext(discovery.localNodeId(), ses.meta(CONN_IDX_META));
        Object prev = ses.addMeta(CONTEXT_META, ctx);
        assert prev == null;
        return ctx;
    }
}

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.security.SecurityUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAuthFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCheckFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientReconnectMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryConnectionCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDiscardMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDuplicateIdMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryLoopbackProblemMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMetricsUpdateMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRedirectToClient;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRingLatencyCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessage;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_LATE_AFFINITY_ASSIGNMENT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_COMPACT_FOOTER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_USE_BINARY_STRING_SER_VER_2;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_USE_DFLT_SUID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.AUTH_FAILED;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.CONNECTED;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.CONNECTING;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.DISCONNECTED;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.DISCONNECTING;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.LEFT;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.STOPPING;
import static org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessage.STATUS_OK;
import static org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessage.STATUS_RECON;

/**
 * Message worker for discovery messages processing.
 */
class RingMessageWorker extends MessageWorker<TcpDiscoveryAbstractMessage> {
    private ServerImpl server;
    /** Next node. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    public TcpDiscoveryNode next;

    /** Pending messages. */
    private final PendingMessages pendingMsgs = new PendingMessages();

    /** Last message that updated topology. */
    private TcpDiscoveryAbstractMessage lastMsg;

    /** Force pending messages send. */
    private boolean forceSndPending;

    /** Socket. */
    Socket sock;

    /** Output stream. */
    OutputStream out;

    /** Last time status message has been sent. */
    long lastTimeStatusMsgSent;

    /** Incoming metrics check frequency. */
    long metricsCheckFreq;

    /** Last time metrics update message has been sent. */
    long lastTimeMetricsUpdateMsgSent;

    /** Time when the last status message has been sent. */
    long lastTimeConnCheckMsgSent;

    /** Flag that keeps info on whether the threshold is reached or not. */
    private boolean failureThresholdReached;

    /** Connection check frequency. */
    private long connCheckFreq;

    /** Connection check threshold. */
    private long connCheckThreshold;

    /** */
    private long lastRingMsgTime;

    /**
     * @param log Logger.
     */
    RingMessageWorker(ServerImpl server, IgniteLogger log) {
        super("tcp-disco-msg-worker", log, 10,
            server.spi.ignite() instanceof IgniteEx ? ((IgniteEx) server.spi.ignite()).context().workersRegistry() : null,
                server.spi);
        this.server = server;

        metricsCheckFreq = 3 * server.spi.metricsUpdateFreq + 50;

        initConnectionCheckFrequency();
    }

    /**
     * Adds message to queue.
     *
     * @param msg Message to add.
     */
    void addMessage(TcpDiscoveryAbstractMessage msg) {
        TcpDiscoveryImpl.DebugLogger log = server.messageLogger(msg);

        if ((msg instanceof TcpDiscoveryStatusCheckMessage ||
            msg instanceof TcpDiscoveryJoinRequestMessage ||
            msg instanceof TcpDiscoveryCustomEventMessage ||
            msg instanceof TcpDiscoveryClientReconnectMessage) &&
            queue.contains(msg)) {

            server.log.info("Ignoring duplicate message: " + msg);

            return;
        }

        if (msg.highPriority())
            queue.addFirst(msg);
        else
            queue.add(msg);

        if (log.isDebugEnabled())
            log.debug("Message has been added to queue: " + msg);
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException {
        Throwable err = null;

        try {
            super.body();
        }
        catch (InterruptedException e) {
            if (!server.spi.isNodeStopping0() && server.spiStateCopy() != DISCONNECTING)
                err = e;

            throw e;
        }
        catch (Throwable e) {
            if (!server.spi.isNodeStopping0() && server.spiStateCopy() != DISCONNECTING) {
                final Ignite ignite = server.spi.ignite();

                if (ignite != null) {
                    U.error(log, "TcpDiscoverSpi's message worker thread failed abnormally. " +
                        "Stopping the node in order to prevent cluster wide instability.", e);

                    new Thread(new Runnable() {
                        @Override public void run() {
                            try {
                                IgnitionEx.stop(ignite.name(), true, true);

                                U.log(log, "Stopped the node successfully in response to TcpDiscoverySpi's " +
                                    "message worker thread abnormal termination.");
                            }
                            catch (Throwable e) {
                                U.error(log, "Failed to stop the node in response to TcpDiscoverySpi's " +
                                    "message worker thread abnormal termination.", e);
                            }
                        }
                    }, "node-stop-thread").start();
                }
            }

            err = e;

            // Must be processed by IgniteSpiThread as well.
            throw e;
        }
        finally {
            if (server.spi.ignite() instanceof IgniteEx) {
                if (err == null && !server.spi.isNodeStopping0()&& server.spiStateCopy() != DISCONNECTING)
                    err = new IllegalStateException("Worker " + name() + " is terminated unexpectedly.");

                FailureProcessor failure = ((IgniteEx) server.spi.ignite()).context().failure();

                if (err instanceof OutOfMemoryError)
                    failure.process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    failure.process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }
    }

    /**
     * Initializes connection check frequency. Used only when failure detection timeout is enabled.
     */
    private void initConnectionCheckFrequency() {
        if (server.spi.failureDetectionTimeoutEnabled())
            connCheckThreshold = server.spi.failureDetectionTimeout();
        else
            connCheckThreshold = Math.min(server.spi.getSocketTimeout(), server.spi.metricsUpdateFreq);

        for (int i = 3; i > 0; i--) {
            connCheckFreq = connCheckThreshold / i;

            if (connCheckFreq > 10)
                break;
        }

        assert connCheckFreq > 0;

        if (log.isDebugEnabled())
            log.debug("Connection check frequency is calculated: " + connCheckFreq);
    }

    /**
     * @param msg Message to process.
     */
    @Override protected void processMessage(TcpDiscoveryAbstractMessage msg) {
        server.spi.startMessageProcess(msg);

        sendMetricsUpdateMessage();

        TcpDiscoveryImpl.DebugLogger log = server.messageLogger(msg);

        if (log.isDebugEnabled())
            log.debug("Processing message [cls=" + msg.getClass().getSimpleName() + ", id=" + msg.id() + ']');

        if (server.debugMode)
            server.debugLog(msg, "Processing message [cls=" + msg.getClass().getSimpleName() + ", id=" + msg.id() + ']');

        boolean ensured = server.spi.ensured(msg);

        if (!server.locNode.id().equals(msg.senderNodeId()) && ensured)
            lastRingMsgTime = U.currentTimeMillis();

        if (server.locNode.internalOrder() == 0) {
            boolean proc = false;

            if (msg instanceof TcpDiscoveryNodeAddedMessage)
                proc = ((TcpDiscoveryNodeAddedMessage)msg).node().equals(server.locNode);

            if (!proc) {
                if (log.isDebugEnabled()) {
                    log.debug("Ignore message, local node order is not initialized [msg=" + msg +
                        ", locNode=" + server.locNode + ']');
                }

                return;
            }
        }

        server.spi.stats.onMessageProcessingStarted(msg);

        server.processMessageFailedNodes(msg);

        if (msg instanceof TcpDiscoveryJoinRequestMessage)
            processJoinRequestMessage((TcpDiscoveryJoinRequestMessage)msg);

        else if (msg instanceof TcpDiscoveryClientReconnectMessage) {
            if (sendMessageToRemotes(msg))
                sendMessageAcrossRing(msg);
        }

        else if (msg instanceof TcpDiscoveryNodeAddedMessage)
            processNodeAddedMessage((TcpDiscoveryNodeAddedMessage)msg);

        else if (msg instanceof TcpDiscoveryNodeAddFinishedMessage)
            processNodeAddFinishedMessage((TcpDiscoveryNodeAddFinishedMessage)msg);

        else if (msg instanceof TcpDiscoveryNodeLeftMessage)
            processNodeLeftMessage((TcpDiscoveryNodeLeftMessage)msg);

        else if (msg instanceof TcpDiscoveryNodeFailedMessage)
            processNodeFailedMessage((TcpDiscoveryNodeFailedMessage)msg);

        else if (msg instanceof TcpDiscoveryMetricsUpdateMessage)
            processMetricsUpdateMessage((TcpDiscoveryMetricsUpdateMessage)msg);

        else if (msg instanceof TcpDiscoveryStatusCheckMessage)
            processStatusCheckMessage((TcpDiscoveryStatusCheckMessage)msg);

        else if (msg instanceof TcpDiscoveryDiscardMessage)
            processDiscardMessage((TcpDiscoveryDiscardMessage)msg);

        else if (msg instanceof TcpDiscoveryCustomEventMessage)
            processCustomMessage((TcpDiscoveryCustomEventMessage)msg, false);

        else if (msg instanceof TcpDiscoveryClientPingRequest)
            processClientPingRequest((TcpDiscoveryClientPingRequest)msg);

        else if (msg instanceof TcpDiscoveryRingLatencyCheckMessage)
            processRingLatencyCheckMessage((TcpDiscoveryRingLatencyCheckMessage)msg);

        else
            assert false : "Unknown message type: " + msg.getClass().getSimpleName();

        if (msg.senderNodeId() != null && !msg.senderNodeId().equals(server.getLocalNodeId())) {
            // Received a message from remote node.
            server.onMessageExchanged();

            // Reset the failure flag.
            failureThresholdReached = false;
        }

        server.spi.stats.onMessageProcessingFinished(msg);
    }

    /** {@inheritDoc} */
    @Override protected void noMessageLoop() {
        if (server.locNode == null)
            return;

        checkConnection();

        sendMetricsUpdateMessage();

        checkMetricsReceiving();

        checkPendingCustomMessages();

        checkFailedNodesList();
    }

    /**
     * @param msg Message.
     */
    private void sendMessageToClients(TcpDiscoveryAbstractMessage msg) {
        if (redirectToClients(msg)) {
            if (server.spi.ensured(msg))
                server.msgHist.add(msg);

            byte[] msgBytes = null;

            for (ServerImpl.ClientMessageWorker clientMsgWorker : server.clientMsgWorkers.values()) {
                if (msgBytes == null) {
                    try {
                        msgBytes = U.marshal(server.spi.marshaller(), msg);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to marshal message: " + msg, e);

                        break;
                    }
                }

                TcpDiscoveryAbstractMessage msg0 = msg;
                byte[] msgBytes0 = msgBytes;

                if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                    TcpDiscoveryNodeAddedMessage nodeAddedMsg = (TcpDiscoveryNodeAddedMessage)msg;

                    TcpDiscoveryNode node = nodeAddedMsg.node();

                    if (clientMsgWorker.clientNodeId.equals(node.id())) {
                        try {
                            msg0 = U.unmarshal(server.spi.marshaller(), msgBytes,
                                U.resolveClassLoader(server.spi.ignite().configuration()));

                            server.prepareNodeAddedMessage(msg0, clientMsgWorker.clientNodeId, null, null, null);

                            msgBytes0 = null;
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to create message copy: " + msg, e);
                        }
                    }
                }

                clientMsgWorker.addMessage(msg0, msgBytes0);
            }
        }
    }

    /**
     * Sends message across the ring.
     *
     * @param msg Message to send
     */
    @SuppressWarnings({"BreakStatementWithLabel", "LabeledStatement", "ContinueStatementWithLabel"})
    private void sendMessageAcrossRing(TcpDiscoveryAbstractMessage msg) {
        log.info("sendMessageAcrossRing " + msg);

        assert msg != null;

        assert server.ring.hasRemoteNodes();

        for (IgniteInClosure<TcpDiscoveryAbstractMessage> msgLsnr : server.spi.sndMsgLsnrs)
            msgLsnr.apply(msg);

        sendMessageToClients(msg);

        Collection<TcpDiscoveryNode> failedNodes;

        TcpDiscoverySpiState state;

        synchronized (server.mux) {
            failedNodes = U.arrayList(server.failedNodes.keySet());

            state = server.spiState;
        }

        Collection<Throwable> errs = null;

        boolean sent = false;

        boolean newNextNode = false;

        UUID locNodeId = server.getLocalNodeId();

        while (true) {
            TcpDiscoveryNode newNext = server.ring.nextNode(failedNodes);

            if (newNext == null) {
                if (log.isDebugEnabled())
                    log.debug("No next node in topology.");

                if (server.debugMode)
                    server.debugLog(msg, "No next node in topology.");

                if (server.ring.hasRemoteNodes() && !(msg instanceof TcpDiscoveryConnectionCheckMessage) &&
                    !(msg instanceof TcpDiscoveryStatusCheckMessage && msg.creatorNodeId().equals(locNodeId))) {
                    msg.senderNodeId(locNodeId);

                    addMessage(msg);
                }

                break;
            }

            if (!newNext.equals(next)) {
                if (log.isDebugEnabled())
                    log.debug("New next node [newNext=" + newNext + ", formerNext=" + next +
                        ", ring=" + server.ring + ", failedNodes=" + failedNodes + ']');

                if (server.debugMode)
                    server.debugLog(msg, "New next node [newNext=" + newNext + ", formerNext=" + next +
                        ", ring=" + server.ring + ", failedNodes=" + failedNodes + ']');

                U.closeQuiet(sock);

                sock = null;

                next = newNext;

                newNextNode = true;
            }
            else if (log.isTraceEnabled())
                log.trace("Next node remains the same [nextId=" + next.id() +
                    ", nextOrder=" + next.internalOrder() + ']');

            final boolean sameHost = U.sameMacs(server.locNode, next);

            List<InetSocketAddress> locNodeAddrs = U.arrayList(server.locNode.socketAddresses());

            addr: for (InetSocketAddress addr : server.spi.getNodeAddresses(next, sameHost)) {
                long ackTimeout0 = server.spi.getAckTimeout();

                if (locNodeAddrs.contains(addr)){
                    if (log.isDebugEnabled())
                        log.debug("Skip to send message to the local node (probably remote node has the same " +
                            "loopback address that local node): " + addr);

                    continue;
                }

                int reconCnt = 0;

                IgniteSpiOperationTimeoutHelper timeoutHelper = null;

                while (true) {
                    if (sock == null) {
                        if (timeoutHelper == null)
                            timeoutHelper = new IgniteSpiOperationTimeoutHelper(server.spi, true);

                        boolean success = false;

                        boolean openSock = false;

                        // Restore ring.
                        try {
                            long tstamp = U.currentTimeMillis();

                            sock = server.spi.openSocket(addr, timeoutHelper);

                            out = server.spi.socketStream(sock);

                            openSock = true;

                            // Handshake.
                            server.spi.writeToSocket(sock, out, new TcpDiscoveryHandshakeRequest(locNodeId),
                                timeoutHelper.nextTimeoutChunk(server.spi.getSocketTimeout()));

                            TcpDiscoveryHandshakeResponse res = server.spi.readMessage(sock, null,
                                timeoutHelper.nextTimeoutChunk(ackTimeout0));

                            if (locNodeId.equals(res.creatorNodeId())) {
                                if (log.isDebugEnabled())
                                    log.debug("Handshake response from local node: " + res);

                                U.closeQuiet(sock);

                                sock = null;

                                break;
                            }

                            server.spi.stats.onClientSocketInitialized(U.currentTimeMillis() - tstamp);

                            UUID nextId = res.creatorNodeId();

                            long nextOrder = res.order();

                            if (!next.id().equals(nextId)) {
                                // Node with different ID has bounded to the same port.
                                if (log.isDebugEnabled())
                                    log.debug("Failed to restore ring because next node ID received is not as " +
                                        "expected [expectedId=" + next.id() + ", rcvdId=" + nextId + ']');

                                if (server.debugMode)
                                    server.debugLog(msg, "Failed to restore ring because next node ID received is not " +
                                        "as expected [expectedId=" + next.id() + ", rcvdId=" + nextId + ']');

                                break;
                            }
                            else {
                                // ID is as expected. Check node order.
                                if (nextOrder != next.internalOrder()) {
                                    // Is next currently being added?
                                    boolean nextNew = (msg instanceof TcpDiscoveryNodeAddedMessage &&
                                        ((TcpDiscoveryNodeAddedMessage)msg).node().id().equals(nextId));

                                    if (!nextNew)
                                        nextNew = hasPendingAddMessage(nextId);

                                    if (!nextNew) {
                                        if (log.isDebugEnabled())
                                            log.debug("Failed to restore ring because next node order received " +
                                                "is not as expected [expected=" + next.internalOrder() +
                                                ", rcvd=" + nextOrder + ", id=" + next.id() + ']');

                                        if (server.debugMode)
                                            server.debugLog(msg, "Failed to restore ring because next node order " +
                                                "received is not as expected [expected=" + next.internalOrder() +
                                                ", rcvd=" + nextOrder + ", id=" + next.id() + ']');

                                        break;
                                    }
                                }

                                if (log.isDebugEnabled())
                                    log.debug("Initialized connection with next node: " + next.id());

                                if (server.debugMode)
                                    server.debugLog(msg, "Initialized connection with next node: " + next.id());

                                errs = null;

                                success = true;

                                next.lastSuccessfulAddress(addr);
                            }
                        }
                        catch (IOException | IgniteCheckedException e) {
                            if (errs == null)
                                errs = new ArrayList<>();

                            errs.add(e);

                            if (log.isDebugEnabled())
                                U.error(log, "Failed to connect to next node [msg=" + msg
                                    + ", err=" + e.getMessage() + ']', e);

                            server.onException("Failed to connect to next node [msg=" + msg + ", err=" + e + ']', e);

                            if (!openSock)
                                break; // Don't retry if we can not establish connection.

                            if (!server.spi.failureDetectionTimeoutEnabled() && ++reconCnt == server.spi.getReconnectCount())
                                break;

                            if (timeoutHelper.checkFailureTimeoutReached(e))
                                break;
                            else if (!server.spi.failureDetectionTimeoutEnabled() && (e instanceof
                                    SocketTimeoutException || X.hasCause(e, SocketTimeoutException.class))) {
                                ackTimeout0 *= 2;

                                if (!server.checkAckTimeout(ackTimeout0))
                                    break;
                            }

                            continue;
                        }
                        finally {
                            if (!success) {
                                if (log.isDebugEnabled())
                                    log.debug("Closing socket to next: " + next);

                                U.closeQuiet(sock);

                                sock = null;
                            }
                            else {
                                // Resetting timeout control object to let the code below to use a new one
                                // for the next bunch of operations.
                                timeoutHelper = null;
                            }
                        }
                    }

                    try {
                        boolean failure;

                        synchronized (server.mux) {
                            failure = server.failedNodes.size() < failedNodes.size();
                        }

                        assert !forceSndPending || msg instanceof TcpDiscoveryNodeLeftMessage;

                        if (failure || forceSndPending) {
                            if (log.isDebugEnabled())
                                log.debug("Pending messages will be sent [failure=" + failure +
                                    ", newNextNode=" + newNextNode +
                                    ", forceSndPending=" + forceSndPending + ']');

                            if (server.debugMode)
                                server.debugLog(msg, "Pending messages will be sent [failure=" + failure +
                                    ", newNextNode=" + newNextNode +
                                    ", forceSndPending=" + forceSndPending + ']');

                            for (TcpDiscoveryAbstractMessage pendingMsg : pendingMsgs) {
                                long tstamp = U.currentTimeMillis();

                                server.prepareNodeAddedMessage(pendingMsg, next.id(), pendingMsgs.msgs,
                                    pendingMsgs.discardId, pendingMsgs.customDiscardId);

                                if (timeoutHelper == null)
                                    timeoutHelper = new IgniteSpiOperationTimeoutHelper(server.spi, true);

                                try {
                                    server.spi.writeToSocket(sock, out, pendingMsg, timeoutHelper.nextTimeoutChunk(
                                        server.spi.getSocketTimeout()));
                                }
                                finally {
                                    server.clearNodeAddedMessage(pendingMsg);
                                }

                                long tstamp0 = U.currentTimeMillis();

                                int res = server.spi.readReceipt(sock, timeoutHelper.nextTimeoutChunk(ackTimeout0));

                                server.spi.stats.onMessageSent(pendingMsg, tstamp0 - tstamp);

                                if (log.isDebugEnabled())
                                    log.debug("Pending message has been sent to next node [msgId=" + msg.id() +
                                        ", pendingMsgId=" + pendingMsg.id() + ", next=" + next.id() +
                                        ", res=" + res + ']');

                                if (server.debugMode)
                                    server.debugLog(msg, "Pending message has been sent to next node [msgId=" + msg.id() +
                                        ", pendingMsgId=" + pendingMsg.id() + ", next=" + next.id() +
                                        ", res=" + res + ']');

                                // Resetting timeout control object to create a new one for the next bunch of
                                // operations.
                                timeoutHelper = null;
                            }
                        }

                        if (!(msg instanceof TcpDiscoveryConnectionCheckMessage))
                            server.prepareNodeAddedMessage(msg, next.id(), pendingMsgs.msgs, pendingMsgs.discardId,
                                pendingMsgs.customDiscardId);

                        try {
                            SecurityUtils.serializeVersion(1);

                            long tstamp = U.currentTimeMillis();

                            if (timeoutHelper == null)
                                timeoutHelper = new IgniteSpiOperationTimeoutHelper(server.spi, true);

                            if (!failedNodes.isEmpty()) {
                                for (TcpDiscoveryNode failedNode : failedNodes) {
                                    assert !failedNode.equals(next) : failedNode;

                                    msg.addFailedNode(failedNode.id());
                                }
                            }

                            boolean latencyCheck = msg instanceof TcpDiscoveryRingLatencyCheckMessage;

                            if (latencyCheck && log.isInfoEnabled())
                                log.info("Latency check message has been written to socket: " + msg.id());

                            server.spi.writeToSocket(newNextNode ? newNext : next,
                                sock,
                                out,
                                msg,
                                timeoutHelper.nextTimeoutChunk(server.spi.getSocketTimeout()));

                            long tstamp0 = U.currentTimeMillis();

                            int res = server.spi.readReceipt(sock, timeoutHelper.nextTimeoutChunk(ackTimeout0));

                            if (latencyCheck && log.isInfoEnabled())
                                log.info("Latency check message has been acked: " + msg.id());

                            server.spi.stats.onMessageSent(msg, tstamp0 - tstamp);

                            server.onMessageExchanged();

                            TcpDiscoveryImpl.DebugLogger debugLog = server.messageLogger(msg);

                            if (debugLog.isDebugEnabled()) {
                                debugLog.debug("Message has been sent to next node [msg=" + msg +
                                    ", next=" + next.id() +
                                    ", res=" + res + ']');
                            }

                            if (server.debugMode) {
                                server.debugLog(msg, "Message has been sent to next node [msg=" + msg +
                                    ", next=" + next.id() +
                                    ", res=" + res + ']');
                            }
                        }
                        finally {
                            SecurityUtils.restoreDefaultSerializeVersion();

                            server.clearNodeAddedMessage(msg);
                        }

                        registerPendingMessage(msg);

                        sent = true;

                        break addr;
                    }
                    catch (IOException | IgniteCheckedException e) {
                        if (errs == null)
                            errs = new ArrayList<>();

                        errs.add(e);

                        if (log.isDebugEnabled())
                            U.error(log, "Failed to send message to next node [next=" + next.id() + ", msg=" + msg +
                                ", err=" + e + ']', e);

                        server.onException("Failed to send message to next node [next=" + next.id() + ", msg=" + msg + ']',
                            e);

                        if (timeoutHelper.checkFailureTimeoutReached(e))
                            break;

                        if (!server.spi.failureDetectionTimeoutEnabled()) {
                            if (++reconCnt == server.spi.getReconnectCount())
                                break;
                            else if (e instanceof SocketTimeoutException ||
                                X.hasCause(e, SocketTimeoutException.class)) {
                                ackTimeout0 *= 2;

                                if (!server.checkAckTimeout(ackTimeout0))
                                    break;
                            }
                        }
                    }
                    finally {
                        forceSndPending = false;

                        if (!sent) {
                            if (log.isDebugEnabled())
                                log.debug("Closing socket to next (not sent): " + next);

                            U.closeQuiet(sock);

                            sock = null;

                            if (log.isDebugEnabled())
                                log.debug("Message has not been sent [next=" + next.id() + ", msg=" + msg +
                                    (!server.spi.failureDetectionTimeoutEnabled() ? ", i=" + reconCnt : "") + ']');
                        }
                    }
                } // Try to reconnect.
            } // Iterating node's addresses.

            if (!sent) {
                if (!failedNodes.contains(next)) {
                    failedNodes.add(next);

                    if (state == CONNECTED) {
                        Exception err = errs != null ?
                            U.exceptionWithSuppressed("Failed to send message to next node [msg=" + msg +
                                ", next=" + U.toShortString(next) + ']', errs) :
                            null;

                        // If node existed on connection initialization we should check
                        // whether it has not gone yet.
                        U.warn(log, "Failed to send message to next node [msg=" + msg + ", next=" + next +
                            ", errMsg=" + (err != null ? err.getMessage() : "N/A") + ']');
                    }
                }

                next = null;

                errs = null;
            }
            else
                break;
        }

        synchronized (server.mux) {
            failedNodes.removeAll(server.failedNodes.keySet());
        }

        if (!failedNodes.isEmpty()) {
            if (state == CONNECTED) {
                if (!sent && log.isDebugEnabled())
                    // Message has not been sent due to some problems.
                    log.debug("Message has not been sent: " + msg);

                if (log.isDebugEnabled())
                    log.debug("Detected failed nodes: " + failedNodes);
            }

            synchronized (server.mux) {
                for (TcpDiscoveryNode failedNode : failedNodes) {
                    if (!server.failedNodes.containsKey(failedNode))
                        server.failedNodes.put(failedNode, locNodeId);
                }

                for (TcpDiscoveryNode failedNode : failedNodes)
                    server.failedNodesMsgSent.add(failedNode.id());
            }

            for (TcpDiscoveryNode n : failedNodes)
                server.msgWorker.addMessage(new TcpDiscoveryNodeFailedMessage(locNodeId, n.id(), n.internalOrder()));

            if (!sent) {
                assert next == null : next;

                if (log.isDebugEnabled())
                    log.debug("Pending messages will be resent to local node");

                if (server.debugMode)
                    server.debugLog(msg, "Pending messages will be resent to local node");

                for (TcpDiscoveryAbstractMessage pendingMsg : pendingMsgs) {
                    server.prepareNodeAddedMessage(pendingMsg, locNodeId, pendingMsgs.msgs, pendingMsgs.discardId,
                        pendingMsgs.customDiscardId);

                    pendingMsg.senderNodeId(locNodeId);

                    server.msgWorker.addMessage(pendingMsg);

                    if (log.isDebugEnabled())
                        log.debug("Pending message has been sent to local node [msg=" + msg.id() +
                            ", pendingMsgId=" + pendingMsg + ']');

                    if (server.debugMode) {
                        server.debugLog(msg, "Pending message has been sent to local node [msg=" + msg.id() +
                            ", pendingMsgId=" + pendingMsg + ']');
                    }
                }
            }

            LT.warn(log, "Local node has detected failed nodes and started cluster-wide procedure. " +
                    "To speed up failure detection please see 'Failure Detection' section under javadoc" +
                    " for 'TcpDiscoverySpi'");
        }
    }

    /**
     * @param msg Message.
     * @return Whether to redirect message to client nodes.
     */
    private boolean redirectToClients(TcpDiscoveryAbstractMessage msg) {
        return msg.verified() && U.hasDeclaredAnnotation(msg, TcpDiscoveryRedirectToClient.class);
    }

    /**
     * Registers pending message.
     *
     * @param msg Message to register.
     */
    private void registerPendingMessage(TcpDiscoveryAbstractMessage msg) {
        assert msg != null;

        if (server.spi.ensured(msg)) {
            pendingMsgs.add(msg);

            server.spi.stats.onPendingMessageRegistered();

            if (log.isDebugEnabled())
                log.debug("Pending message has been registered: " + msg.id());
        }
    }

    /**
     * Checks whether pending messages queue contains unprocessed {@link TcpDiscoveryNodeAddedMessage} for
     * the node with {@code nodeId}.
     *
     * @param nodeId Node ID.
     * @return {@code true} if contains, {@code false} otherwise.
     */
    private boolean hasPendingAddMessage(UUID nodeId) {
        if (pendingMsgs.msgs.isEmpty())
            return false;

        for (PendingMessage pendingMsg : pendingMsgs.msgs) {
            if (pendingMsg.msg instanceof TcpDiscoveryNodeAddedMessage) {
                TcpDiscoveryNodeAddedMessage addMsg = (TcpDiscoveryNodeAddedMessage)pendingMsg.msg;

                if (addMsg.node().id().equals(nodeId) && addMsg.id().compareTo(pendingMsgs.discardId) > 0)
                    return true;
            }
        }

        return false;
    }

    /**
     * Processes join request message.
     *
     * @param msg Join request message.
     */
    private void processJoinRequestMessage(final TcpDiscoveryJoinRequestMessage msg) {
        assert msg != null;

        final TcpDiscoveryNode node = msg.node();

        final UUID locNodeId = server.getLocalNodeId();

        if (!msg.client()) {
            boolean rmtHostLoopback = node.socketAddresses().size() == 1 &&
                node.socketAddresses().iterator().next().getAddress().isLoopbackAddress();

            // This check is performed by the node joining node is connected to, but not by coordinator
            // because loopback problem message is sent directly to the joining node which may be unavailable
            // if coordinator resides on another host.
            if (server.spi.locHost.isLoopbackAddress() != rmtHostLoopback) {
                String firstNode = rmtHostLoopback ? "remote" : "local";

                String secondNode = rmtHostLoopback ? "local" : "remote";

                String errMsg = "Failed to add node to topology because " + firstNode +
                    " node is configured to use loopback address, but " + secondNode + " node is not " +
                    "(consider changing 'localAddress' configuration parameter) " +
                    "[locNodeAddrs=" + U.addressesAsString(server.locNode) +
                    ", rmtNodeAddrs=" + U.addressesAsString(node) + ']';

                LT.warn(log, errMsg);

                // Always output in debug.
                if (log.isDebugEnabled())
                    log.debug(errMsg);

                try {
                    trySendMessageDirectly(node, new TcpDiscoveryLoopbackProblemMessage(
                        locNodeId, server.locNode.addresses(), server.locNode.hostNames()));
                }
                catch (IgniteSpiException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send loopback problem message to node " +
                            "[node=" + node + ", err=" + e.getMessage() + ']');

                    server.onException("Failed to send loopback problem message to node " +
                        "[node=" + node + ", err=" + e.getMessage() + ']', e);
                }

                // Ignore join request.
                return;
            }
        }

        if (server.isLocalNodeCoordinator()) {
            TcpDiscoveryNode existingNode = server.ring.node(node.id());

            if (existingNode != null) {
                if (!node.socketAddresses().equals(existingNode.socketAddresses())) {
                    if (!server.pingNode(existingNode)) {
                        U.warn(log, "Sending node failed message for existing node: " + node);

                        addMessage(new TcpDiscoveryNodeFailedMessage(locNodeId,
                            existingNode.id(), existingNode.internalOrder()));

                        // Ignore this join request since existing node is about to fail
                        // and new node can continue.
                        return;
                    }

                    try {
                        trySendMessageDirectly(node, new TcpDiscoveryDuplicateIdMessage(locNodeId,
                            existingNode));
                    }
                    catch (IgniteSpiException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send duplicate ID message to node " +
                                "[node=" + node + ", existingNode=" + existingNode +
                                ", err=" + e.getMessage() + ']');

                        server.onException("Failed to send duplicate ID message to node " +
                            "[node=" + node + ", existingNode=" + existingNode + ']', e);
                    }

                    // Output warning.
                    LT.warn(log, "Ignoring join request from node (duplicate ID) [node=" + node +
                        ", existingNode=" + existingNode + ']');

                    // Ignore join request.
                    return;
                }

                if (msg.client()) {
                    TcpDiscoveryClientReconnectMessage reconMsg = new TcpDiscoveryClientReconnectMessage(node.id(),
                        node.clientRouterNodeId(),
                        null);

                    reconMsg.verify(server.getLocalNodeId());

                    Collection<TcpDiscoveryAbstractMessage> msgs = server.msgHist.messages(null, node);

                    if (msgs != null) {
                        reconMsg.pendingMessages(msgs);

                        reconMsg.success(true);
                    }

                    if (log.isDebugEnabled())
                        log.debug("Send reconnect message to already joined client " +
                            "[clientNode=" + existingNode + ", msg=" + reconMsg + ']');

                    if (server.getLocalNodeId().equals(node.clientRouterNodeId())) {
                        ServerImpl.ClientMessageWorker wrk = server.clientMsgWorkers.get(node.id());

                        if (wrk != null)
                            wrk.addMessage(reconMsg);
                        else if (log.isDebugEnabled())
                            log.debug("Failed to find client message worker " +
                                "[clientNode=" + existingNode + ", msg=" + reconMsg + ']');
                    }
                    else {
                        if (sendMessageToRemotes(reconMsg))
                            sendMessageAcrossRing(reconMsg);
                    }
                }
                else if (log.isDebugEnabled())
                    log.debug("Ignoring join request message since node is already in topology: " + msg);

                return;
            }

            if (server.spi.nodeAuth != null) {
                // Authenticate node first.
                try {
                    SecurityCredentials cred = server.unmarshalCredentials(node);

                    SecurityContext subj = server.spi.nodeAuth.authenticateNode(node, cred);

                    if (subj == null) {
                        // Node has not pass authentication.
                        LT.warn(log, "Authentication failed [nodeId=" + node.id() +
                            ", addrs=" + U.addressesAsString(node) + ']');

                        // Always output in debug.
                        if (log.isDebugEnabled())
                            log.debug("Authentication failed [nodeId=" + node.id() + ", addrs=" +
                                U.addressesAsString(node));

                        try {
                            trySendMessageDirectly(node, new TcpDiscoveryAuthFailedMessage(locNodeId,
                                server.spi.locHost));
                        }
                        catch (IgniteSpiException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to send unauthenticated message to node " +
                                    "[node=" + node + ", err=" + e.getMessage() + ']');

                            server.onException("Failed to send unauthenticated message to node " +
                                "[node=" + node + ", err=" + e.getMessage() + ']', e);
                        }

                        // Ignore join request.
                        return;
                    }
                    else {
                        String authFailedMsg = null;

                        if (!(subj instanceof Serializable)) {
                            // Node has not pass authentication.
                            LT.warn(log, "Authentication subject is not Serializable [nodeId=" + node.id() +
                                ", addrs=" + U.addressesAsString(node) + ']');

                            authFailedMsg = "Authentication subject is not serializable";
                        }
                        else if (node.clientRouterNodeId() == null &&
                            !subj.systemOperationAllowed(SecurityPermission.JOIN_AS_SERVER))
                            authFailedMsg = "Node is not authorised to join as a server node";

                        if (authFailedMsg != null) {
                            // Always output in debug.
                            if (log.isDebugEnabled())
                                log.debug(authFailedMsg + " [nodeId=" + node.id() +
                                    ", addrs=" + U.addressesAsString(node));

                            try {
                                trySendMessageDirectly(node, new TcpDiscoveryAuthFailedMessage(locNodeId,
                                    server.spi.locHost));
                            }
                            catch (IgniteSpiException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send unauthenticated message to node " +
                                        "[node=" + node + ", err=" + e.getMessage() + ']');
                            }

                            // Ignore join request.
                            return;
                        }

                        // Stick in authentication subject to node (use security-safe attributes for copy).
                        Map<String, Object> attrs = new HashMap<>(node.getAttributes());

                        attrs.put(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2, U.marshal(server.spi.marshaller(), subj));
                        attrs.put(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT, server.marshalWithSecurityVersion(subj, 1));

                        node.setAttributes(attrs);
                    }
                }
                catch (IgniteException | IgniteCheckedException e) {
                    LT.error(log, e, "Authentication failed [nodeId=" + node.id() + ", addrs=" +
                        U.addressesAsString(node) + ']');

                    if (log.isDebugEnabled())
                        log.debug("Failed to authenticate node (will ignore join request) [node=" + node +
                            ", err=" + e + ']');

                    server.onException("Failed to authenticate node (will ignore join request) [node=" + node +
                        ", err=" + e + ']', e);

                    // Ignore join request.
                    return;
                }
            }

            IgniteNodeValidationResult err;

            err = server.spi.getSpiContext().validateNode(node);

            if (err == null)
                err = server.spi.getSpiContext().validateNode(node, msg.gridDiscoveryData().unmarshalJoiningNodeData(server.spi.marshaller(), U.resolveClassLoader(server.spi.ignite().configuration()), false, log));

            if (err != null) {
                final IgniteNodeValidationResult err0 = err;

                if (log.isDebugEnabled())
                    log.debug("Node validation failed [res=" + err + ", node=" + node + ']');

                server.utilityPool.execute(
                    new Runnable() {
                        @Override public void run() {
                            boolean ping = node.id().equals(err0.nodeId()) ? server.pingNode(node) : server.pingNode(err0.nodeId());

                            if (!ping) {
                                if (log.isDebugEnabled())
                                    log.debug("Conflicting node has already left, need to wait for event. " +
                                        "Will ignore join request for now since it will be recent [req=" + msg +
                                        ", err=" + err0.message() + ']');

                                // Ignore join request.
                                return;
                            }

                            LT.warn(log, err0.message());

                            // Always output in debug.
                            if (log.isDebugEnabled())
                                log.debug(err0.message());

                            try {
                                trySendMessageDirectly(node,
                                    new TcpDiscoveryCheckFailedMessage(err0.nodeId(), err0.sendMessage()));
                            }
                            catch (IgniteSpiException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send hash ID resolver validation failed message to node " +
                                        "[node=" + node + ", err=" + e.getMessage() + ']');

                                server.onException("Failed to send hash ID resolver validation failed message to node " +
                                    "[node=" + node + ", err=" + e.getMessage() + ']', e);
                            }
                        }
                    }
                );

                // Ignore join request.
                return;
            }

            final String locMarsh = server.locNode.attribute(ATTR_MARSHALLER);
            final String rmtMarsh = node.attribute(ATTR_MARSHALLER);

            if (!F.eq(locMarsh, rmtMarsh)) {
                server.utilityPool.execute(
                    new Runnable() {
                        @Override public void run() {
                            String errMsg = "Local node's marshaller differs from remote node's marshaller " +
                                "(to make sure all nodes in topology have identical marshaller, " +
                                "configure marshaller explicitly in configuration) " +
                                "[locMarshaller=" + locMarsh + ", rmtMarshaller=" + rmtMarsh +
                                ", locNodeAddrs=" + U.addressesAsString(server.locNode) +
                                ", rmtNodeAddrs=" + U.addressesAsString(node) +
                                ", locNodeId=" + server.locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                            LT.warn(log, errMsg);

                            // Always output in debug.
                            if (log.isDebugEnabled())
                                log.debug(errMsg);

                            try {
                                String sndMsg = "Local node's marshaller differs from remote node's marshaller " +
                                    "(to make sure all nodes in topology have identical marshaller, " +
                                    "configure marshaller explicitly in configuration) " +
                                    "[locMarshaller=" + rmtMarsh + ", rmtMarshaller=" + locMarsh +
                                    ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                    ", rmtNodeAddr=" + U.addressesAsString(server.locNode) + ", locNodeId=" + node.id() +
                                    ", rmtNodeId=" + server.locNode.id() + ']';

                                trySendMessageDirectly(node,
                                    new TcpDiscoveryCheckFailedMessage(locNodeId, sndMsg));
                            }
                            catch (IgniteSpiException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send marshaller check failed message to node " +
                                        "[node=" + node + ", err=" + e.getMessage() + ']');

                                server.onException("Failed to send marshaller check failed message to node " +
                                    "[node=" + node + ", err=" + e.getMessage() + ']', e);
                            }
                        }
                    }
                );

                // Ignore join request.
                return;
            }

            // If node have no value for this attribute then we treat it as true.
            final Boolean locMarshUseDfltSuid = server.locNode.attribute(ATTR_MARSHALLER_USE_DFLT_SUID);
            boolean locMarshUseDfltSuidBool = locMarshUseDfltSuid == null ? true : locMarshUseDfltSuid;

            final Boolean rmtMarshUseDfltSuid = node.attribute(ATTR_MARSHALLER_USE_DFLT_SUID);
            boolean rmtMarshUseDfltSuidBool = rmtMarshUseDfltSuid == null ? true : rmtMarshUseDfltSuid;

            Boolean locLateAssign = server.locNode.attribute(ATTR_LATE_AFFINITY_ASSIGNMENT);
            // Can be null only in tests.
            boolean locLateAssignBool = locLateAssign != null ? locLateAssign : false;

            if (locMarshUseDfltSuidBool != rmtMarshUseDfltSuidBool) {
                server.utilityPool.execute(
                    new Runnable() {
                        @Override public void run() {
                            String errMsg = "Local node's " + IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID +
                                " property value differs from remote node's value " +
                                "(to make sure all nodes in topology have identical marshaller settings, " +
                                "configure system property explicitly) " +
                                "[locMarshUseDfltSuid=" + locMarshUseDfltSuid +
                                ", rmtMarshUseDfltSuid=" + rmtMarshUseDfltSuid +
                                ", locNodeAddrs=" + U.addressesAsString(server.locNode) +
                                ", rmtNodeAddrs=" + U.addressesAsString(node) +
                                ", locNodeId=" + server.locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                            String sndMsg = "Local node's " + IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID +
                                " property value differs from remote node's value " +
                                "(to make sure all nodes in topology have identical marshaller settings, " +
                                "configure system property explicitly) " +
                                "[locMarshUseDfltSuid=" + rmtMarshUseDfltSuid +
                                ", rmtMarshUseDfltSuid=" + locMarshUseDfltSuid +
                                ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                ", rmtNodeAddr=" + U.addressesAsString(server.locNode) + ", locNodeId=" + node.id() +
                                ", rmtNodeId=" + server.locNode.id() + ']';

                            nodeCheckError(
                                node,
                                errMsg,
                                sndMsg);
                        }
                    });

                // Ignore join request.
                return;
            }

            // Validate compact footer flags.
            Boolean locMarshCompactFooter = server.locNode.attribute(ATTR_MARSHALLER_COMPACT_FOOTER);
            final boolean locMarshCompactFooterBool = locMarshCompactFooter != null ? locMarshCompactFooter : false;

            Boolean rmtMarshCompactFooter = node.attribute(ATTR_MARSHALLER_COMPACT_FOOTER);
            final boolean rmtMarshCompactFooterBool = rmtMarshCompactFooter != null ? rmtMarshCompactFooter : false;

            if (locMarshCompactFooterBool != rmtMarshCompactFooterBool) {
                server.utilityPool.execute(
                    new Runnable() {
                        @Override public void run() {
                            String errMsg = "Local node's binary marshaller \"compactFooter\" property differs from " +
                                "the same property on remote node (make sure all nodes in topology have the same value " +
                                "of \"compactFooter\" property) [locMarshallerCompactFooter=" + locMarshCompactFooterBool +
                                ", rmtMarshallerCompactFooter=" + rmtMarshCompactFooterBool +
                                ", locNodeAddrs=" + U.addressesAsString(server.locNode) +
                                ", rmtNodeAddrs=" + U.addressesAsString(node) +
                                ", locNodeId=" + server.locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                            String sndMsg = "Local node's binary marshaller \"compactFooter\" property differs from " +
                                "the same property on remote node (make sure all nodes in topology have the same value " +
                                "of \"compactFooter\" property) [locMarshallerCompactFooter=" + rmtMarshCompactFooterBool +
                                ", rmtMarshallerCompactFooter=" + locMarshCompactFooterBool +
                                ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                ", rmtNodeAddr=" + U.addressesAsString(server.locNode) + ", locNodeId=" + node.id() +
                                ", rmtNodeId=" + server.locNode.id() + ']';

                            nodeCheckError(
                                node,
                                errMsg,
                                sndMsg);
                        }
                    });

                // Ignore join request.
                return;
            }

            // Validate String serialization mechanism used by the BinaryMarshaller.
            final Boolean locMarshStrSerialVer2 = server.locNode.attribute(ATTR_MARSHALLER_USE_BINARY_STRING_SER_VER_2);
            final boolean locMarshStrSerialVer2Bool = locMarshStrSerialVer2 != null ? locMarshStrSerialVer2 : false;

            final Boolean rmtMarshStrSerialVer2 = node.attribute(ATTR_MARSHALLER_USE_BINARY_STRING_SER_VER_2);
            final boolean rmtMarshStrSerialVer2Bool = rmtMarshStrSerialVer2 != null ? rmtMarshStrSerialVer2 : false;

            if (locMarshStrSerialVer2Bool != rmtMarshStrSerialVer2Bool) {
                server.utilityPool.execute(
                    new Runnable() {
                        @Override public void run() {
                            String errMsg = "Local node's " + IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2 +
                                " property value differs from remote node's value " +
                                "(to make sure all nodes in topology have identical marshaller settings, " +
                                "configure system property explicitly) " +
                                "[locMarshStrSerialVer2=" + locMarshStrSerialVer2 +
                                ", rmtMarshStrSerialVer2=" + rmtMarshStrSerialVer2 +
                                ", locNodeAddrs=" + U.addressesAsString(server.locNode) +
                                ", rmtNodeAddrs=" + U.addressesAsString(node) +
                                ", locNodeId=" + server.locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                            String sndMsg = "Local node's " + IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2 +
                                " property value differs from remote node's value " +
                                "(to make sure all nodes in topology have identical marshaller settings, " +
                                "configure system property explicitly) " +
                                "[locMarshStrSerialVer2=" + rmtMarshStrSerialVer2 +
                                ", rmtMarshStrSerialVer2=" + locMarshStrSerialVer2 +
                                ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                ", rmtNodeAddr=" + U.addressesAsString(server.locNode) + ", locNodeId=" + node.id() +
                                ", rmtNodeId=" + server.locNode.id() + ']';

                            nodeCheckError(
                                node,
                                errMsg,
                                sndMsg);
                        }
                    });

                // Ignore join request.
                return;
            }

            Boolean rmtLateAssign = node.attribute(ATTR_LATE_AFFINITY_ASSIGNMENT);
            // Can be null only in tests.
            boolean rmtLateAssignBool = rmtLateAssign != null ? rmtLateAssign : false;

            if (locLateAssignBool != rmtLateAssignBool) {
                String errMsg = "Local node's cache affinity assignment mode differs from " +
                    "the same property on remote node (make sure all nodes in topology have the same " +
                    "cache affinity assignment mode) [locLateAssign=" + locLateAssignBool +
                    ", rmtLateAssign=" + rmtLateAssignBool +
                    ", locNodeAddrs=" + U.addressesAsString(server.locNode) +
                    ", rmtNodeAddrs=" + U.addressesAsString(node) +
                    ", locNodeId=" + server.locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                String sndMsg = "Local node's cache affinity assignment mode differs from " +
                    "the same property on remote node (make sure all nodes in topology have the same " +
                    "cache affinity assignment mode) [locLateAssign=" + rmtLateAssignBool +
                    ", rmtLateAssign=" + locLateAssign +
                    ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                    ", rmtNodeAddr=" + U.addressesAsString(server.locNode) + ", locNodeId=" + node.id() +
                    ", rmtNodeId=" + server.locNode.id() + ']';

                nodeCheckError(node, errMsg, sndMsg);

                // Ignore join request.
                return;
            }

            final Boolean locSrvcCompatibilityEnabled = server.locNode.attribute(ATTR_SERVICES_COMPATIBILITY_MODE);

            final Boolean rmtSrvcCompatibilityEnabled = node.attribute(ATTR_SERVICES_COMPATIBILITY_MODE);

            if (!F.eq(locSrvcCompatibilityEnabled, rmtSrvcCompatibilityEnabled)) {
                server.utilityPool.execute(
                    new Runnable() {
                        @Override public void run() {
                            String errMsg = "Local node's " + IGNITE_SERVICES_COMPATIBILITY_MODE +
                                " property value differs from remote node's value " +
                                "(to make sure all nodes in topology have identical IgniteServices compatibility mode, " +
                                "configure system property explicitly) " +
                                "[locSrvcCompatibilityEnabled=" + locSrvcCompatibilityEnabled +
                                ", rmtSrvcCompatibilityEnabled=" + rmtSrvcCompatibilityEnabled +
                                ", locNodeAddrs=" + U.addressesAsString(server.locNode) +
                                ", rmtNodeAddrs=" + U.addressesAsString(node) +
                                ", locNodeId=" + server.locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                            String sndMsg = "Local node's " + IGNITE_SERVICES_COMPATIBILITY_MODE +
                                " property value differs from remote node's value " +
                                "(to make sure all nodes in topology have identical IgniteServices compatibility mode, " +
                                "configure system property explicitly) " +
                                "[locSrvcCompatibilityEnabled=" + rmtSrvcCompatibilityEnabled +
                                ", rmtSrvcCompatibilityEnabled=" + locSrvcCompatibilityEnabled +
                                ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                ", rmtNodeAddr=" + U.addressesAsString(server.locNode) + ", locNodeId=" + node.id() +
                                ", rmtNodeId=" + server.locNode.id() + ']';

                            nodeCheckError(
                                node,
                                errMsg,
                                sndMsg);
                        }
                    });

                // Ignore join request.
                return;
            }

            // Handle join.
            node.internalOrder(server.ring.nextNodeOrder());

            if (log.isDebugEnabled())
                log.debug("Internal order has been assigned to node: " + node);

            DiscoveryDataPacket data = msg.gridDiscoveryData();

            TcpDiscoveryNodeAddedMessage nodeAddedMsg = new TcpDiscoveryNodeAddedMessage(locNodeId,
                node, data, server.spi.gridStartTime);

            nodeAddedMsg.client(msg.client());

            processNodeAddedMessage(nodeAddedMsg);
        }
        else if (sendMessageToRemotes(msg))
            sendMessageAcrossRing(msg);
    }

    /**
     * @param node Node.
     * @param name Attribute name.
     * @param dflt Default value.
     * @return Attribute value.
     */
    private boolean booleanAttribute(ClusterNode node, String name, boolean dflt) {
        Boolean attr = node.attribute(name);

        return attr != null ? attr : dflt;
    }

    /**
     * @param node Joining node.
     * @param errMsg Message to log.
     * @param sndMsg Message to send.
     */
    private void nodeCheckError(TcpDiscoveryNode node, String errMsg, String sndMsg) {
        LT.warn(log, errMsg);

        // Always output in debug.
        if (log.isDebugEnabled())
            log.debug(errMsg);

        try {
            trySendMessageDirectly(node, new TcpDiscoveryCheckFailedMessage(server.locNode.id(), sndMsg));
        }
        catch (IgniteSpiException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send marshaller check failed message to node " +
                    "[node=" + node + ", err=" + e.getMessage() + ']');

            server.onException("Failed to send marshaller check failed message to node " +
                "[node=" + node + ", err=" + e.getMessage() + ']', e);
        }
    }

    /**
     * Tries to send a message to all node's available addresses.
     *
     * @param node Node to send message to.
     * @param msg Message.
     * @throws IgniteSpiException Last failure if all attempts failed.
     */
    private void trySendMessageDirectly(TcpDiscoveryNode node, TcpDiscoveryAbstractMessage msg)
        throws IgniteSpiException {
        if (node.clientRouterNodeId() != null) {
            TcpDiscoveryNode routerNode = server.ring.node(node.clientRouterNodeId());

            if (routerNode == null)
                throw new IgniteSpiException("Router node for client does not exist: " + node);

            if (routerNode.clientRouterNodeId() != null)
                throw new IgniteSpiException("Router node is a client node: " + node);

            if (routerNode.id().equals(server.getLocalNodeId())) {
                ServerImpl.ClientMessageWorker worker = server.clientMsgWorkers.get(node.id());

                if (worker == null)
                    throw new IgniteSpiException("Client node already disconnected: " + node);

                msg.verify(server.getLocalNodeId()); // Client worker require verified messages.

                worker.addMessage(msg);

                return;
            }

            trySendMessageDirectly(routerNode, msg);

            return;
        }

        IgniteSpiException ex = null;

        for (InetSocketAddress addr : server.spi.getNodeAddresses(node, U.sameMacs(server.locNode, node))) {
            try {
                IgniteSpiOperationTimeoutHelper timeoutHelper = new IgniteSpiOperationTimeoutHelper(server.spi, true);

                server.sendMessageDirectly(msg, addr, timeoutHelper);

                node.lastSuccessfulAddress(addr);

                ex = null;

                break;
            }
            catch (IgniteSpiException e) {
                ex = e;
            }
        }

        if (ex != null)
            throw ex;
    }

    /**
     * Processes node added message.
     *
     * For coordinator node method marks the messages as verified for rest of nodes to apply the
     * changes this message is issued for.
     *
     * Node added message is processed by other nodes only after coordinator verification.
     *
     * @param msg Node added message.
     * @deprecated Due to current protocol node add process cannot be dropped in the middle of the ring,
     *      if new node auth fails due to config inconsistency. So, we need to finish add
     *      and only then initiate failure.
     */
    @Deprecated
    private void processNodeAddedMessage(TcpDiscoveryNodeAddedMessage msg) {
        assert msg != null;

        TcpDiscoveryNode node = msg.node();

        assert node != null;

        if (node.internalOrder() < server.locNode.internalOrder()) {
            if (log.isDebugEnabled())
                log.debug("Discarding node added message since local node's order is greater " +
                    "[node=" + node + ", locNode=" + server.locNode + ", msg=" + msg + ']');

            return;
        }

        UUID locNodeId = server.getLocalNodeId();

        if (server.isLocalNodeCoordinator()) {
            if (msg.verified()) {
                server.spi.stats.onRingMessageReceived(msg);

                TcpDiscoveryNodeAddFinishedMessage addFinishMsg = new TcpDiscoveryNodeAddFinishedMessage(locNodeId,
                    node.id());

                if (node.clientRouterNodeId() != null) {
                    addFinishMsg.clientDiscoData(msg.gridDiscoveryData());

                    addFinishMsg.clientNodeAttributes(node.attributes());
                }

                processNodeAddFinishedMessage(addFinishMsg);

                addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id(), false));

                return;
            }

            msg.verify(locNodeId);
        }
        else if (!locNodeId.equals(node.id()) && server.ring.node(node.id()) != null) {
            // Local node already has node from message in local topology.
            // Just pass it to coordinator via the ring.
            if (sendMessageToRemotes(msg))
                sendMessageAcrossRing(msg);

            if (log.isDebugEnabled()) {
                log.debug("Local node already has node being added. Passing TcpDiscoveryNodeAddedMessage to " +
                    "coordinator for final processing [ring=" + server.ring + ", node=" + node + ", locNode="
                    + server.locNode + ", msg=" + msg + ']');
            }

            if (server.debugMode) {
                server.debugLog(msg, "Local node already has node being added. Passing TcpDiscoveryNodeAddedMessage to " +
                    "coordinator for final processing [ring=" + server.ring + ", node=" + node + ", locNode="
                    + server.locNode + ", msg=" + msg + ']');
            }

            return;
        }

        if (msg.verified() && !locNodeId.equals(node.id())) {
            if (node.internalOrder() <= server.ring.maxInternalOrder()) {
                if (log.isDebugEnabled())
                    log.debug("Discarding node added message since new node's order is less than " +
                        "max order in ring [ring=" + server.ring + ", node=" + node + ", locNode=" + server.locNode +
                        ", msg=" + msg + ']');

                if (server.debugMode)
                    server.debugLog(msg, "Discarding node added message since new node's order is less than " +
                        "max order in ring [ring=" + server.ring + ", node=" + node + ", locNode=" + server.locNode +
                        ", msg=" + msg + ']');

                return;
            }

            synchronized (server.mux) {
                server.joiningNodes.add(node.id());
            }

            if (!server.isLocalNodeCoordinator() && server.spi.nodeAuth != null && server.spi.nodeAuth.isGlobalNodeAuthentication()) {
                boolean authFailed = true;

                try {
                    SecurityCredentials cred = server.unmarshalCredentials(node);

                    if (cred == null) {
                        if (log.isDebugEnabled())
                            log.debug(
                                "Skipping global authentication for node (security credentials not found, " +
                                    "probably, due to coordinator has older version) " +
                                    "[nodeId=" + node.id() +
                                    ", addrs=" + U.addressesAsString(node) +
                                    ", coord=" + server.ring.coordinator() + ']');

                        authFailed = false;
                    }
                    else {
                        SecurityContext subj = server.spi.nodeAuth.authenticateNode(node, cred);

                        byte[] subjBytes = node.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT);
                        byte[] subjBytesV2 = node.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2);

                        SecurityContext coordSubj;

                        try {
                            if (subjBytesV2 == null)
                                SecurityUtils.serializeVersion(1);

                            coordSubj = U.unmarshal(server.spi.marshaller(),
                                subjBytesV2 != null ? subjBytesV2 : subjBytes,
                                U.resolveClassLoader(server.spi.ignite().configuration()));
                        }
                        finally {
                            SecurityUtils.restoreDefaultSerializeVersion();
                        }

                        if (!server.permissionsEqual(coordSubj.subject().permissions(), subj.subject().permissions())) {
                            // Node has not pass authentication.
                            LT.warn(log, "Authentication failed [nodeId=" + node.id() +
                                ", addrs=" + U.addressesAsString(node) + ']');

                            // Always output in debug.
                            if (log.isDebugEnabled())
                                log.debug("Authentication failed [nodeId=" + node.id() + ", addrs=" +
                                    U.addressesAsString(node));
                        }
                        else
                            // Node will not be kicked out.
                            authFailed = false;
                    }
                }
                catch (IgniteException | IgniteCheckedException e) {
                    U.error(log, "Failed to verify node permissions consistency (will drop the node): " + node, e);
                }
                finally {
                    if (authFailed) {
                        try {
                            trySendMessageDirectly(node, new TcpDiscoveryAuthFailedMessage(locNodeId,
                                server.spi.locHost));
                        }
                        catch (IgniteSpiException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to send unauthenticated message to node " +
                                    "[node=" + node + ", err=" + e.getMessage() + ']');

                            server.onException("Failed to send unauthenticated message to node " +
                                "[node=" + node + ", err=" + e.getMessage() + ']', e);
                        }

                        addMessage(new TcpDiscoveryNodeFailedMessage(locNodeId, node.id(),
                            node.internalOrder()));
                    }
                }
            }

            if (msg.client())
                node.clientAliveTime(server.spi.clientFailureDetectionTimeout());

            boolean topChanged = server.ring.add(node);

            if (topChanged) {
                assert !node.visible() : "Added visible node [node=" + node + ", locNode=" + server.locNode + ']';

                DiscoveryDataPacket dataPacket = msg.gridDiscoveryData();

                assert dataPacket != null : msg;

                dataPacket.joiningNodeClient(msg.client());

                if (dataPacket.hasJoiningNodeData())
                    server.spi.onExchange(dataPacket, U.resolveClassLoader(server.spi.ignite().configuration()));

                server.spi.collectExchangeData(dataPacket);

                server.processMessageFailedNodes(msg);
            }

            if (log.isDebugEnabled())
                log.debug("Added node to local ring [added=" + topChanged + ", node=" + node +
                    ", ring=" + server.ring + ']');
        }

        if (msg.verified() && locNodeId.equals(node.id())) {
            DiscoveryDataPacket dataPacket;

            synchronized (server.mux) {
                if (server.spiState == CONNECTING && server.locNode.internalOrder() != node.internalOrder()) {
                    // Initialize topology.
                    Collection<TcpDiscoveryNode> top = msg.topology();

                    if (top != null && !top.isEmpty()) {
                        server.spi.gridStartTime = msg.gridStartTime();

                        if (server.spi.nodeAuth != null && server.spi.nodeAuth.isGlobalNodeAuthentication()) {
                            TcpDiscoveryAbstractMessage authFail =
                                new TcpDiscoveryAuthFailedMessage(locNodeId, server.spi.locHost);

                            try {
                                byte[] rmSubj = node.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT);
                                byte[] locSubj = server.locNode.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT);

                                byte[] rmSubjV2 = node.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2);
                                byte[] locSubjV2 = server.locNode.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2);

                                int ver = 1; // Compatible version.

                                if (rmSubjV2 != null && locSubjV2 != null) {
                                    rmSubj = rmSubjV2;
                                    locSubj = locSubjV2;

                                    ver = 0; // Default version.
                                }

                                SecurityContext rmCrd = server.unmarshalWithSecurityVersion(rmSubj, ver);
                                SecurityContext locCrd = server.unmarshalWithSecurityVersion(locSubj, ver);

                                if (!server.permissionsEqual(locCrd.subject().permissions(),
                                    rmCrd.subject().permissions())) {
                                    // Node has not pass authentication.
                                    LT.warn(log,
                                        "Failed to authenticate local node " +
                                            "(local authentication result is different from rest of topology) " +
                                            "[nodeId=" + node.id() + ", addrs=" + U.addressesAsString(node) + ']');

                                    server.joinRes.set(authFail);

                                    server.spiState = AUTH_FAILED;

                                    server.mux.notifyAll();

                                    return;
                                }
                            }
                            catch (IgniteCheckedException e) {
                                U.error(log, "Failed to verify node permissions consistency (will drop the node): " + node, e);

                                server.joinRes.set(authFail);

                                server.spiState = AUTH_FAILED;

                                server.mux.notifyAll();

                                return;
                            }
                        }

                        for (TcpDiscoveryNode n : top) {
                            assert n.internalOrder() < node.internalOrder() :
                                "Invalid node [topNode=" + n + ", added=" + node + ']';

                            // Make all preceding nodes and local node visible.
                            n.visible(true);
                        }

                        synchronized (server.mux) {
                            server.joiningNodes.clear();
                        }

                        server.locNode.setAttributes(node.attributes());

                        server.locNode.visible(true);

                        // Restore topology with all nodes visible.
                        server.ring.restoreTopology(top, node.internalOrder());

                        if (log.isDebugEnabled())
                            log.debug("Restored topology from node added message: " + server.ring);

                        dataPacket = msg.gridDiscoveryData();

                        server.topHist.clear();
                        server.topHist.putAll(msg.topologyHistory());

                        pendingMsgs.reset(msg.messages(), msg.discardedMessageId(),
                            msg.discardedCustomMessageId());

                        // Clear data to minimize message size.
                        msg.messages(null, null, null);
                        msg.topology(null);
                        msg.topologyHistory(null);
                        msg.clearDiscoveryData();
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node added message with empty topology: " + msg);

                        return;
                    }
                }
                else  {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node added message (this message has already been processed) " +
                            "[spiState=" + server.spiState +
                            ", msg=" + msg +
                            ", locNode=" + server.locNode + ']');

                    return;
                }
            }

            // Notify outside of synchronized block.
            if (dataPacket != null)
                server.spi.onExchange(dataPacket, U.resolveClassLoader(server.spi.ignite().configuration()));

            server.processMessageFailedNodes(msg);
        }

        if (sendMessageToRemotes(msg))
            sendMessageAcrossRing(msg);
    }

    /**
     * Processes node add finished message.
     *
     * @param msg Node add finished message.
     */
    private void processNodeAddFinishedMessage(TcpDiscoveryNodeAddFinishedMessage msg) {
        assert msg != null;

        UUID nodeId = msg.nodeId();

        assert nodeId != null;

        TcpDiscoveryNode node = server.ring.node(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Discarding node add finished message since node is not found " +
                    "[msg=" + msg + ']');

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Node to finish add: " + node);

        boolean locNodeCoord = server.isLocalNodeCoordinator();

        UUID locNodeId = server.getLocalNodeId();

        if (locNodeCoord) {
            if (msg.verified()) {
                server.spi.stats.onRingMessageReceived(msg);

                addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id(), false));

                return;
            }

            if (node.visible() && node.order() != 0) {
                if (log.isDebugEnabled())
                    log.debug("Discarding node add finished message since node has already been added " +
                        "[node=" + node + ", msg=" + msg + ']');

                return;
            }
            else
                msg.topologyVersion(server.ring.incrementTopologyVersion());

            msg.verify(locNodeId);
        }

        long topVer = msg.topologyVersion();

        boolean fireEvt = false;

        if (msg.verified()) {
            assert topVer > 0 : "Invalid topology version: " + msg;

            if (node.order() == 0)
                node.order(topVer);

            if (!node.visible()) {
                node.visible(true);

                fireEvt = true;
            }
        }

        synchronized (server.mux) {
            server.joiningNodes.remove(nodeId);
        }

        TcpDiscoverySpiState state = server.spiStateCopy();

        if (msg.verified() && !locNodeId.equals(nodeId) && state != CONNECTING && fireEvt) {
            server.spi.stats.onNodeJoined();

            // Make sure that node with greater order will never get EVT_NODE_JOINED
            // on node with less order.
            assert node.internalOrder() > server.locNode.internalOrder() : "Invalid order [node=" + node +
                ", locNode=" + server.locNode + ", msg=" + msg + ", ring=" + server.ring + ']';

            if (server.spi.locNodeVer.equals(node.version()))
                node.version(server.spi.locNodeVer);

            if (!locNodeCoord) {
                boolean b = server.ring.topologyVersion(topVer);

                assert b : "Topology version has not been updated: [ring=" + server.ring + ", msg=" + msg +
                    ", lastMsg=" + lastMsg + ", spiState=" + state + ']';

                if (log.isDebugEnabled())
                    log.debug("Topology version has been updated: [ring=" + server.ring + ", msg=" + msg + ']');

                lastMsg = msg;
            }

            if (state == CONNECTED)
                server.notifyDiscovery(EVT_NODE_JOINED, topVer, node);

            try {
                if (server.spi.ipFinder.isShared() && locNodeCoord && node.clientRouterNodeId() == null)
                    server.spi.ipFinder.registerAddresses(node.socketAddresses());
            }
            catch (IgniteSpiException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to register new node address [node=" + node +
                        ", err=" + e.getMessage() + ']');

                server.onException("Failed to register new node address [node=" + node +
                    ", err=" + e.getMessage() + ']', e);
            }
        }

        if (msg.verified() && locNodeId.equals(nodeId) && state == CONNECTING) {
            assert node != null;

            assert topVer > 0 : "Invalid topology version: " + msg;

            server.ring.topologyVersion(topVer);

            node.order(topVer);

            synchronized (server.mux) {
                server.spiState = CONNECTED;

                server.mux.notifyAll();
            }

            // Discovery manager must create local joined event before spiStart completes.
            server.notifyDiscovery(EVT_NODE_JOINED, topVer, server.locNode);
        }

        if (sendMessageToRemotes(msg))
            sendMessageAcrossRing(msg);

        checkPendingCustomMessages();
    }

    /**
     * Processes latency check message.
     *
     * @param msg Latency check message.
     */
    private void processRingLatencyCheckMessage(TcpDiscoveryRingLatencyCheckMessage msg) {
        assert msg != null;

        if (msg.maxHopsReached()) {
            if (log.isInfoEnabled())
                log.info("Latency check has been discarded (max hops reached) [id=" + msg.id() +
                    ", maxHops=" + msg.maxHops() + ']');

            return;
        }

        if (log.isInfoEnabled())
            log.info("Latency check processing: " + msg.id());

        if (sendMessageToRemotes(msg))
            sendMessageAcrossRing(msg);
        else {
            if (log.isInfoEnabled())
                log.info("Latency check has been discarded (no remote nodes): " + msg.id());
        }
    }

    /**
     * Processes node left message.
     *
     * @param msg Node left message.
     */
    private void processNodeLeftMessage(TcpDiscoveryNodeLeftMessage msg) {
        assert msg != null;

        UUID locNodeId = server.getLocalNodeId();

        UUID leavingNodeId = msg.creatorNodeId();

        if (locNodeId.equals(leavingNodeId)) {
            if (msg.senderNodeId() == null) {
                synchronized (server.mux) {
                    if (log.isDebugEnabled())
                        log.debug("Starting local node stop procedure.");

                    server.spiState = STOPPING;

                    server.mux.notifyAll();
                }
            }

            if (msg.verified() || !server.ring.hasRemoteNodes() || msg.senderNodeId() != null) {
                if (server.spi.ipFinder.isShared() && !server.ring.hasRemoteNodes()) {
                    try {
                        server.spi.ipFinder.unregisterAddresses(
                            U.resolveAddresses(server.spi.getAddressResolver(), server.locNode.socketAddresses()));
                    }
                    catch (IgniteSpiException e) {
                        U.error(log, "Failed to unregister local node address from IP finder.", e);
                    }
                }

                synchronized (server.mux) {
                    if (server.spiState == STOPPING) {
                        server.spiState = LEFT;

                        server.mux.notifyAll();
                    }
                }

                return;
            }

            sendMessageAcrossRing(msg);

            return;
        }

        if (server.ring.node(msg.senderNodeId()) == null) {
            if (log.isDebugEnabled())
                log.debug("Discarding node left message since sender node is not in topology: " + msg);

            return;
        }

        TcpDiscoveryNode leavingNode = server.ring.node(leavingNodeId);

        if (leavingNode != null) {
            synchronized (server.mux) {
                server.leavingNodes.add(leavingNode);
            }
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Discarding node left message since node was not found: " + msg);

            return;
        }

        boolean locNodeCoord = server.isLocalNodeCoordinator();

        if (locNodeCoord) {
            if (msg.verified()) {
                server.spi.stats.onRingMessageReceived(msg);

                addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id(), false));

                return;
            }

            msg.verify(locNodeId);
        }

        if (msg.verified() && !locNodeId.equals(leavingNodeId)) {
            TcpDiscoveryNode leftNode = server.ring.removeNode(leavingNodeId);

            server.interruptPing(leavingNode);

            assert leftNode != null : msg;

            if (log.isDebugEnabled())
                log.debug("Removed node from topology: " + leftNode);

            long topVer;

            if (locNodeCoord) {
                topVer = server.ring.incrementTopologyVersion();

                msg.topologyVersion(topVer);
            }
            else {
                topVer = msg.topologyVersion();

                assert topVer > 0 : "Topology version is empty for message: " + msg;

                boolean b = server.ring.topologyVersion(topVer);

                assert b : "Topology version has not been updated: [ring=" + server.ring + ", msg=" + msg +
                    ", lastMsg=" + lastMsg + ", spiState=" + server.spiStateCopy() + ']';

                if (log.isDebugEnabled())
                    log.debug("Topology version has been updated: [ring=" + server.ring + ", msg=" + msg + ']');

                lastMsg = msg;
            }

            if (msg.client()) {
                ServerImpl.ClientMessageWorker wrk = server.clientMsgWorkers.remove(leavingNodeId);

                if (wrk != null)
                    wrk.addMessage(msg);
            }
            else if (leftNode.equals(next) && sock != null) {
                try {
                    server.spi.writeToSocket(sock, out, msg, server.spi.failureDetectionTimeoutEnabled() ?
                        server.spi.failureDetectionTimeout() : server.spi.getSocketTimeout());

                    if (log.isDebugEnabled())
                        log.debug("Sent verified node left message to leaving node: " + msg);
                }
                catch (IgniteCheckedException | IOException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send verified node left message to leaving node [msg=" + msg +
                            ", err=" + e.getMessage() + ']');

                    server.onException("Failed to send verified node left message to leaving node [msg=" + msg +
                        ", err=" + e.getMessage() + ']', e);
                }
                finally {
                    forceSndPending = true;

                    next = null;

                    U.closeQuiet(sock);
                }
            }

            synchronized (server.mux) {
                server.joiningNodes.remove(leftNode.id());
            }

            server.spi.stats.onNodeLeft();

            server.notifyDiscovery(EVT_NODE_LEFT, topVer, leftNode);

            synchronized (server.mux) {
                server.failedNodes.remove(leftNode);

                server.leavingNodes.remove(leftNode);

                server.failedNodesMsgSent.remove(leftNode.id());
            }
        }

        if (sendMessageToRemotes(msg)) {
            try {
                sendMessageAcrossRing(msg);
            }
            finally {
                forceSndPending = false;
            }
        }
        else {
            forceSndPending = false;

            if (log.isDebugEnabled())
                log.debug("Unable to send message across the ring (topology has no remote nodes): " + msg);

            U.closeQuiet(sock);
        }

        checkPendingCustomMessages();
    }

    /**
     * @param msg Message to send.
     * @return {@code True} if message should be send across the ring.
     */
    private boolean sendMessageToRemotes(TcpDiscoveryAbstractMessage msg) {
        if (server.ring.hasRemoteNodes())
            return true;

        sendMessageToClients(msg);

        return false;
    }

    /**
     * Processes node failed message.
     *
     * @param msg Node failed message.
     */
    private void processNodeFailedMessage(TcpDiscoveryNodeFailedMessage msg) {
        assert msg != null;

        UUID sndId = msg.senderNodeId();

        if (sndId != null) {
            TcpDiscoveryNode sndNode = server.ring.node(sndId);

            if (sndNode == null) {
                if (log.isDebugEnabled())
                    log.debug("Discarding node failed message sent from unknown node: " + msg);

                return;
            }
            else {
                boolean contains;

                UUID creatorId = msg.creatorNodeId();

                assert creatorId != null : msg;

                synchronized (server.mux) {
                    contains = server.failedNodes.containsKey(sndNode) || server.ring.node(creatorId) == null;
                }

                if (contains) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node failed message sent from node which is about to fail: " + msg);

                    return;
                }
            }
        }

        UUID failedNodeId = msg.failedNodeId();
        long order = msg.order();

        TcpDiscoveryNode failedNode = server.ring.node(failedNodeId);

        if (failedNode != null && failedNode.internalOrder() != order) {
            if (log.isDebugEnabled())
                log.debug("Ignoring node failed message since node internal order does not match " +
                    "[msg=" + msg + ", node=" + failedNode + ']');

            return;
        }

        if (failedNode != null) {
            assert !failedNode.isLocal() || !msg.verified() : msg;

            boolean skipUpdateFailedNodes = msg.force() && !msg.verified();

            if (!skipUpdateFailedNodes) {
                synchronized (server.mux) {
                    if (!server.failedNodes.containsKey(failedNode))
                        server.failedNodes.put(failedNode, msg.senderNodeId() != null ? msg.senderNodeId() : server.getLocalNodeId());
                }
            }
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Discarding node failed message since node was not found: " + msg);

            return;
        }

        boolean locNodeCoord = server.isLocalNodeCoordinator();

        UUID locNodeId = server.getLocalNodeId();

        if (locNodeCoord) {
            if (msg.verified()) {
                server.spi.stats.onRingMessageReceived(msg);

                addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id(), false));

                return;
            }

            msg.verify(locNodeId);
        }

        if (msg.verified()) {
            failedNode = server.ring.removeNode(failedNodeId);

            server.interruptPing(failedNode);

            assert failedNode != null;

            long topVer;

            if (locNodeCoord) {
                topVer = server.ring.incrementTopologyVersion();

                msg.topologyVersion(topVer);
            }
            else {
                topVer = msg.topologyVersion();

                assert topVer > 0 : "Topology version is empty for message: " + msg;

                boolean b = server.ring.topologyVersion(topVer);

                assert b : "Topology version has not been updated: [ring=" + server.ring + ", msg=" + msg +
                    ", lastMsg=" + lastMsg + ", spiState=" + server.spiStateCopy() + ']';

                if (log.isDebugEnabled())
                    log.debug("Topology version has been updated: [ring=" + server.ring + ", msg=" + msg + ']');

                lastMsg = msg;
            }

            synchronized (server.mux) {
                server.failedNodes.remove(failedNode);

                server.leavingNodes.remove(failedNode);

                server.failedNodesMsgSent.remove(failedNode.id());

                if (!msg.force()) { // ClientMessageWorker will stop after sending force fail message.
                    ServerImpl.ClientMessageWorker worker = server.clientMsgWorkers.remove(failedNode.id());

                    if (worker != null && worker.runner() != null)
                        worker.runner().interrupt();
                }
            }

            if (msg.warning() != null && !msg.creatorNodeId().equals(server.getLocalNodeId())) {
                ClusterNode creatorNode = server.ring.node(msg.creatorNodeId());

                U.warn(log, "Received EVT_NODE_FAILED event with warning [" +
                    "nodeInitiatedEvt=" + (creatorNode != null ? creatorNode : msg.creatorNodeId()) +
                    ", msg=" + msg.warning() + ']');
            }

            synchronized (server.mux) {
                server.joiningNodes.remove(failedNode.id());
            }

            server.notifyDiscovery(EVT_NODE_FAILED, topVer, failedNode);

            server.spi.stats.onNodeFailed();
        }

        if (sendMessageToRemotes(msg))
            sendMessageAcrossRing(msg);
        else {
            if (log.isDebugEnabled())
                log.debug("Unable to send message across the ring (topology has no remote nodes): " + msg);

            U.closeQuiet(sock);
        }

        checkPendingCustomMessages();
    }

    /**
     * Processes status check message.
     *
     * @param msg Status check message.
     */
    private void processStatusCheckMessage(final TcpDiscoveryStatusCheckMessage msg) {
        assert msg != null;

        UUID locNodeId = server.getLocalNodeId();

        if (msg.failedNodeId() != null) {
            if (locNodeId.equals(msg.failedNodeId())) {
                if (log.isDebugEnabled())
                    log.debug("Status check message discarded (suspect node is local node).");

                return;
            }

            if (locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() != null) {
                if (log.isDebugEnabled())
                    log.debug("Status check message discarded (local node is the sender of the status message).");

                return;
            }

            if (server.isLocalNodeCoordinator() && server.ring.node(msg.creatorNodeId()) == null) {
                if (log.isDebugEnabled())
                    log.debug("Status check message discarded (creator node is not in topology).");

                return;
            }
        }
        else {
            if (server.isLocalNodeCoordinator() && !locNodeId.equals(msg.creatorNodeId())) {
                // Local node is real coordinator, it should respond and discard message.
                if (server.ring.node(msg.creatorNodeId()) != null) {
                    // Sender is in topology, send message via ring.
                    msg.status(STATUS_OK);

                    sendMessageAcrossRing(msg);
                }
                else {
                    // Sender is not in topology, it should reconnect.
                    msg.status(STATUS_RECON);

                    server.utilityPool.execute(new Runnable() {
                        @Override public void run() {
                            if (server.spiState == DISCONNECTED) {
                                if (log.isDebugEnabled())
                                    log.debug("Ignoring status check request, SPI is already disconnected: " + msg);

                                return;
                            }

                            TcpDiscoveryStatusCheckMessage msg0 = msg;

                            if (F.contains(msg.failedNodes(), msg.creatorNodeId())) {
                                msg0 = new TcpDiscoveryStatusCheckMessage(msg);

                                msg0.failedNodes(null);

                                for (UUID failedNodeId : msg.failedNodes()) {
                                    if (!failedNodeId.equals(msg.creatorNodeId()))
                                        msg0.addFailedNode(failedNodeId);
                                }
                            }

                            try {
                                trySendMessageDirectly(msg0.creatorNode(), msg0);

                                if (log.isDebugEnabled())
                                    log.debug("Responded to status check message " +
                                        "[recipient=" + msg0.creatorNodeId() + ", status=" + msg0.status() + ']');
                            }
                            catch (IgniteSpiException e) {
                                if (e.hasCause(SocketException.class)) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to respond to status check message (connection " +
                                            "refused) [recipient=" + msg0.creatorNodeId() + ", status=" +
                                            msg0.status() + ']');

                                    server.onException("Failed to respond to status check message (connection refused) " +
                                        "[recipient=" + msg0.creatorNodeId() + ", status=" + msg0.status() + ']', e);
                                }
                                else if (!server.spi.isNodeStopping0()) {
                                    if (server.pingNode(msg0.creatorNode()))
                                        // Node exists and accepts incoming connections.
                                        U.error(log, "Failed to respond to status check message [recipient=" +
                                            msg0.creatorNodeId() + ", status=" + msg0.status() + ']', e);
                                    else if (log.isDebugEnabled()) {
                                        log.debug("Failed to respond to status check message (did the node stop?)" +
                                            "[recipient=" + msg0.creatorNodeId() +
                                            ", status=" + msg0.status() + ']');
                                    }
                                }
                            }
                        }
                    });
                }

                return;
            }

            if (locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() == null &&
                U.currentTimeMillis() - server.locNode.lastUpdateTime() < server.spi.metricsUpdateFreq) {
                if (log.isDebugEnabled())
                    log.debug("Status check message discarded (local node receives updates).");

                return;
            }

            if (locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() == null &&
                server.spiStateCopy() != CONNECTED) {
                if (log.isDebugEnabled())
                    log.debug("Status check message discarded (local node is not connected to topology).");

                return;
            }

            if (locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() != null) {
                if (server.spiStateCopy() != CONNECTED)
                    return;

                if (msg.status() == STATUS_OK) {
                    if (log.isDebugEnabled())
                        log.debug("Received OK status response from coordinator: " + msg);
                }
                else if (msg.status() == STATUS_RECON) {
                    U.warn(log, "Node is out of topology (probably, due to short-time network problems).");

                    server.notifyDiscovery(EVT_NODE_SEGMENTED, server.ring.topologyVersion(), server.locNode);

                    return;
                }
                else if (log.isDebugEnabled())
                    log.debug("Status value was not updated in status response: " + msg);

                // Discard the message.
                return;
            }
        }

        if (sendMessageToRemotes(msg))
            sendMessageAcrossRing(msg);
    }

    /**
     * Processes regular metrics update message.
     *
     * @param msg Metrics update message.
     */
    private void processMetricsUpdateMessage(TcpDiscoveryMetricsUpdateMessage msg) {
        assert msg != null;

        assert !msg.client();

        UUID locNodeId = server.getLocalNodeId();

        if (server.ring.node(msg.creatorNodeId()) == null) {
            if (log.isDebugEnabled())
                log.debug("Discarding metrics update message issued by unknown node [msg=" + msg +
                    ", ring=" + server.ring + ']');

            return;
        }

        if (server.isLocalNodeCoordinator() && !locNodeId.equals(msg.creatorNodeId())) {
            if (log.isDebugEnabled())
                log.debug("Discarding metrics update message issued by non-coordinator node: " + msg);

            return;
        }

        if (!server.isLocalNodeCoordinator() && locNodeId.equals(msg.creatorNodeId())) {
            if (log.isDebugEnabled())
                log.debug("Discarding metrics update message issued by local node (node is no more coordinator): " +
                    msg);

            return;
        }

        if (locNodeId.equals(msg.creatorNodeId()) && !hasMetrics(msg, locNodeId) && msg.senderNodeId() != null) {
            if (log.isTraceEnabled())
                log.trace("Discarding metrics update message that has made two passes: " + msg);

            return;
        }

        long tstamp = U.currentTimeMillis();

        if (server.spiStateCopy() == CONNECTED) {
            if (msg.hasMetrics()) {
                for (Map.Entry<UUID, TcpDiscoveryMetricsUpdateMessage.MetricsSet> e : msg.metrics().entrySet()) {
                    UUID nodeId = e.getKey();

                    TcpDiscoveryMetricsUpdateMessage.MetricsSet metricsSet = e.getValue();

                    Map<Integer, CacheMetrics> cacheMetrics = msg.hasCacheMetrics(nodeId) ?
                        msg.cacheMetrics().get(nodeId) : Collections.<Integer, CacheMetrics>emptyMap();

                    updateMetrics(nodeId, metricsSet.metrics(), cacheMetrics, tstamp);

                    for (T2<UUID, ClusterMetrics> t : metricsSet.clientMetrics())
                        updateMetrics(t.get1(), t.get2(), cacheMetrics, tstamp);
                }
            }
        }

        if (sendMessageToRemotes(msg)) {
            if ((locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() == null ||
                !hasMetrics(msg, locNodeId)) && server.spiStateCopy() == CONNECTED) {
                // Message is on its first ring or just created on coordinator.
                msg.setMetrics(locNodeId, server.spi.metricsProvider.metrics());
                msg.setCacheMetrics(locNodeId, server.spi.metricsProvider.cacheMetrics());

                for (Map.Entry<UUID, ServerImpl.ClientMessageWorker> e : server.clientMsgWorkers.entrySet()) {
                    UUID nodeId = e.getKey();
                    ClusterMetrics metrics = e.getValue().metrics();

                    if (metrics != null)
                        msg.setClientMetrics(locNodeId, nodeId, metrics);

                    msg.addClientNodeId(nodeId);
                }
            }
            else {
                // Message is on its second ring.
                ServerImpl.removeMetrics(msg, locNodeId);

                Collection<UUID> clientNodeIds = msg.clientNodeIds();

                for (TcpDiscoveryNode clientNode : server.ring.clientNodes()) {
                    if (clientNode.visible()) {
                        if (clientNodeIds.contains(clientNode.id()))
                            clientNode.clientAliveTime(server.spi.clientFailureDetectionTimeout());
                        else {
                            boolean aliveCheck = clientNode.isClientAlive();

                            if (!aliveCheck && server.isLocalNodeCoordinator()) {
                                boolean failedNode;

                                synchronized (server.mux) {
                                    failedNode = server.failedNodes.containsKey(clientNode);
                                }

                                if (!failedNode) {
                                    U.warn(log, "Failing client node due to not receiving metrics updates " +
                                        "from client node within " +
                                        "'IgniteConfiguration.clientFailureDetectionTimeout' " +
                                        "(consider increasing configuration property) " +
                                        "[timeout=" + server.spi.clientFailureDetectionTimeout() + ", node=" + clientNode + ']');

                                    TcpDiscoveryNodeFailedMessage nodeFailedMsg = new TcpDiscoveryNodeFailedMessage(
                                        locNodeId, clientNode.id(), clientNode.internalOrder());

                                    processNodeFailedMessage(nodeFailedMsg);
                                }
                            }
                        }
                    }
                }
            }

            if (sendMessageToRemotes(msg))
                sendMessageAcrossRing(msg);
        }
        else {
            server.locNode.lastUpdateTime(tstamp);

            server.notifyDiscovery(EVT_NODE_METRICS_UPDATED, server.ring.topologyVersion(), server.locNode);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param metrics Metrics.
     * @param cacheMetrics Cache metrics.
     * @param tstamp Timestamp.
     */
    private void updateMetrics(UUID nodeId,
        ClusterMetrics metrics,
        Map<Integer, CacheMetrics> cacheMetrics,
        long tstamp)
    {
        assert nodeId != null;
        assert metrics != null;

        TcpDiscoveryNode node = server.ring.node(nodeId);

        if (node != null) {
            node.setMetrics(metrics);
            node.setCacheMetrics(cacheMetrics);

            node.lastUpdateTime(tstamp);

            server.notifyDiscovery(EVT_NODE_METRICS_UPDATED, server.ring.topologyVersion(), node);
        }
        else if (log.isDebugEnabled())
            log.debug("Received metrics from unknown node: " + nodeId);
    }

    /**
     * @param msg Message.
     */
    private boolean hasMetrics(TcpDiscoveryMetricsUpdateMessage msg, UUID nodeId) {
        return msg.hasMetrics(nodeId) || msg.hasCacheMetrics(nodeId);
    }

    /**
     * Processes discard message and discards previously registered pending messages.
     *
     * @param msg Discard message.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    private void processDiscardMessage(TcpDiscoveryDiscardMessage msg) {
        assert msg != null;

        IgniteUuid msgId = msg.msgId();

        assert msgId != null;

        if (server.isLocalNodeCoordinator()) {
            if (!server.getLocalNodeId().equals(msg.verifierNodeId()))
                // Message is not verified or verified by former coordinator.
                msg.verify(server.getLocalNodeId());
            else
                // Discard the message.
                return;
        }

        if (msg.verified())
            pendingMsgs.discard(msgId, msg.customMessageDiscard());

        if (server.ring.hasRemoteNodes())
            sendMessageAcrossRing(msg);
    }

    /**
     * @param msg Message.
     */
    private void processClientPingRequest(final TcpDiscoveryClientPingRequest msg) {
        server.utilityPool.execute(new Runnable() {
            @Override public void run() {
                if (server.spiState == DISCONNECTED) {
                    if (log.isDebugEnabled())
                        log.debug("Ignoring ping request, SPI is already disconnected: " + msg);

                    return;
                }

                final ServerImpl.ClientMessageWorker worker = server.clientMsgWorkers.get(msg.creatorNodeId());

                if (worker == null) {
                    if (log.isDebugEnabled())
                        log.debug("Ping request from dead client node, will be skipped: " + msg.creatorNodeId());
                }
                else {
                    boolean res;

                    try {
                        res = server.pingNode(msg.nodeToPing());
                    } catch (IgniteSpiException e) {
                        log.error("Failed to ping node [nodeToPing=" + msg.nodeToPing() + ']', e);

                        res = false;
                    }

                    TcpDiscoveryClientPingResponse pingRes = new TcpDiscoveryClientPingResponse(
                        server.getLocalNodeId(), msg.nodeToPing(), res);

                    pingRes.verify(server.getLocalNodeId());

                    worker.addMessage(pingRes);
                }
            }
        });
    }

    /**
     * @param msg Message.
     * @param waitForNotification If {@code true} then thread will wait when discovery event notification has finished.
     */
    private void processCustomMessage(TcpDiscoveryCustomEventMessage msg, boolean waitForNotification) {
        log.info("processCustomMessage " + msg);

        if (server.isLocalNodeCoordinator()) {
            boolean delayMsg;

            assert server.ring.minimumNodeVersion() != null : server.ring;

            boolean joiningEmpty;

            synchronized (server.mux) {
                joiningEmpty = server.joiningNodes.isEmpty();
            }

            delayMsg = msg.topologyVersion() == 0L && !joiningEmpty;

            if (delayMsg) {
                if (log.isDebugEnabled()) {
                    synchronized (server.mux) {
                        log.debug("Delay custom message processing, there are joining nodes [msg=" + msg +
                            ", joiningNodes=" + server.joiningNodes + ']');
                    }
                }

                synchronized (server.mux) {
                    server.pendingCustomMsgs.add(msg);
                }

                return;
            }

            if (!msg.verified()) {
                msg.verify(server.getLocalNodeId());
                msg.topologyVersion(server.ring.topologyVersion());

                if (pendingMsgs.procCustomMsgs.add(msg.id())) {
                    notifyDiscoveryListener(msg, waitForNotification);

                    if (sendMessageToRemotes(msg))
                        sendMessageAcrossRing(msg);
                    else
                        processCustomMessage(msg, waitForNotification);
                } else
                    log.info("procCustomMsgs.add false " + msg.id());

                msg.message(null, msg.messageBytes());
            }
            else {
                log.info("Discard " + msg);

                addMessage(new TcpDiscoveryDiscardMessage(server.getLocalNodeId(), msg.id(), true));

                server.spi.stats.onRingMessageReceived(msg);

                DiscoverySpiCustomMessage msgObj = null;

                try {
                    msgObj = msg.message(server.spi.marshaller(), U.resolveClassLoader(server.spi.ignite().configuration()));
                }
                catch (Throwable e) {
                    U.error(log, "Failed to unmarshal discovery custom message.", e);
                }

                if (msgObj != null) {
                    DiscoverySpiCustomMessage nextMsg = msgObj.ackMessage();

                    if (nextMsg != null) {
                        try {
                            TcpDiscoveryCustomEventMessage ackMsg = new TcpDiscoveryCustomEventMessage(
                                    server.getLocalNodeId(), nextMsg, U.marshal(server.spi.marshaller(), nextMsg));

                            ackMsg.topologyVersion(msg.topologyVersion());

                            processCustomMessage(ackMsg, waitForNotification);
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to marshal discovery custom message.", e);
                        }
                    }
                }
            }
        }
        else {
            TcpDiscoverySpiState state0;

            synchronized (server.mux) {
                state0 = server.spiState;
            }

            if (msg.verified() && msg.topologyVersion() != server.ring.topologyVersion()) {
                log.info("Discarding custom event message [msg=" + msg + ", ring=" + server.ring + ']');

                return;
            }

            if (msg.verified() && state0 == CONNECTED && pendingMsgs.procCustomMsgs.add(msg.id())) {
                assert msg.topologyVersion() == server.ring.topologyVersion() :
                    "msg: " + msg + ", topVer=" + server.ring.topologyVersion();

                notifyDiscoveryListener(msg, waitForNotification);
            }

            if (msg.verified())
                msg.message(null, msg.messageBytes());

            if (sendMessageToRemotes(msg))
                sendMessageAcrossRing(msg);
        }
    }

    /**
     * Checks failed nodes list and sends {@link TcpDiscoveryNodeFailedMessage} if failed node is still in the
     * ring and node detected failure left ring.
     */
    private void checkFailedNodesList() {
        List<TcpDiscoveryNodeFailedMessage> msgs = null;

        synchronized (server.mux) {
            if (!server.failedNodes.isEmpty()) {
                for (Iterator<Map.Entry<TcpDiscoveryNode, UUID>> it = server.failedNodes.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry<TcpDiscoveryNode, UUID> e = it.next();

                    TcpDiscoveryNode node = e.getKey();
                    UUID failSndNode = e.getValue();

                    if (server.ring.node(node.id()) == null) {
                        it.remove();

                        continue;
                    }

                    if (!server.nodeAlive(failSndNode) && !server.failedNodesMsgSent.contains(node.id())) {
                        if (msgs == null)
                            msgs = new ArrayList<>();

                        msgs.add(new TcpDiscoveryNodeFailedMessage(server.getLocalNodeId(), node.id(), node.internalOrder()));

                        server.failedNodesMsgSent.add(node.id());
                    }
                }
            }

            if (!server.failedNodesMsgSent.isEmpty()) {
                for (Iterator<UUID> it = server.failedNodesMsgSent.iterator(); it.hasNext(); ) {
                    UUID nodeId = it.next();

                    if (server.ring.node(nodeId) == null)
                        it.remove();
                }
            }
        }

        if (msgs != null) {
            for (TcpDiscoveryNodeFailedMessage msg : msgs) {
                U.warn(log, "Added node failed message for node from failed nodes list: " + msg);

                addMessage(msg);
            }
        }
    }

    /**
     * Checks and flushes custom event messages if no nodes are attempting to join the grid.
     */
    private void checkPendingCustomMessages() {
        boolean joiningEmpty;

        synchronized (server.mux) {
            joiningEmpty = server.joiningNodes.isEmpty();
        }

        if (joiningEmpty && server.isLocalNodeCoordinator()) {
            TcpDiscoveryCustomEventMessage msg;

            while ((msg = pollPendingCustomeMessage()) != null) {
                log.info("pollPendingCustomMessage " + msg);

                processCustomMessage(msg, true);
            }
        }
    }

    /**
     * @return Pending custom message.
     */
    @Nullable
    private TcpDiscoveryCustomEventMessage pollPendingCustomeMessage() {
        synchronized (server.mux) {
            return server.pendingCustomMsgs.poll();
        }
    }

    /**
     * @param msg Custom message.
     * @param waitForNotification If {@code true} thread will wait when discovery event notification has finished.
     */
    private void notifyDiscoveryListener(TcpDiscoveryCustomEventMessage msg, boolean waitForNotification) {
        DiscoverySpiListener lsnr = server.spi.lsnr;

        TcpDiscoverySpiState spiState = server.spiStateCopy();

        Map<Long, Collection<ClusterNode>> hist;

        synchronized (server.mux) {
            hist = new TreeMap<>(server.topHist);
        }

        Collection<ClusterNode> snapshot = hist.get(msg.topologyVersion());

        if (lsnr != null && (spiState == CONNECTED || spiState == DISCONNECTING)) {
            TcpDiscoveryNode node = server.ring.node(msg.creatorNodeId());

            if (node == null)
                return;

            DiscoverySpiCustomMessage msgObj;

            try {
                msgObj = msg.message(server.spi.marshaller(), U.resolveClassLoader(server.spi.ignite().configuration()));
            }
            catch (Throwable t) {
                throw new IgniteException("Failed to unmarshal discovery custom message: " + msg, t);
            }

            IgniteFuture<?> fut = lsnr.onDiscovery(DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT,
                msg.topologyVersion(),
                node,
                snapshot,
                hist,
                msgObj);

            if (waitForNotification || msgObj.isMutable()) {
                log.info("Await future for " + msgObj);

                fut.get();
            } else {
                log.info("Skip future for " + msgObj);

            }

            if (msgObj.isMutable()) {
                try {
                    msg.message(msgObj, U.marshal(server.spi.marshaller(), msgObj));
                }
                catch (Throwable t) {
                    throw new IgniteException("Failed to marshal mutable discovery message: " + msgObj, t);
                }
            }
        }
    }

    /**
     * Sends metrics update message if needed.
     */
    private void sendMetricsUpdateMessage() {
        long elapsed = (lastTimeMetricsUpdateMsgSent + server.spi.metricsUpdateFreq) - U.currentTimeMillis();

        if (elapsed > 0 || !server.isLocalNodeCoordinator())
            return;

        TcpDiscoveryMetricsUpdateMessage msg = new TcpDiscoveryMetricsUpdateMessage(server.getConfiguredNodeId());

        msg.verify(server.getLocalNodeId());

        server.msgWorker.addMessage(msg);

        lastTimeMetricsUpdateMsgSent = U.currentTimeMillis();
    }

    /**
     * Checks the last time a metrics update message received. If the time is bigger than {@code metricsCheckFreq}
     * than {@link TcpDiscoveryStatusCheckMessage} is sent across the ring.
     */
    private void checkMetricsReceiving() {
        if (lastTimeStatusMsgSent < server.locNode.lastUpdateTime())
            lastTimeStatusMsgSent = server.locNode.lastUpdateTime();

        long updateTime = Math.max(lastTimeStatusMsgSent, lastRingMsgTime);

        long elapsed = (updateTime + metricsCheckFreq) - U.currentTimeMillis();

        if (elapsed > 0)
            return;

        server.msgWorker.addMessage(new TcpDiscoveryStatusCheckMessage(server.locNode, null));

        lastTimeStatusMsgSent = U.currentTimeMillis();
    }

    /**
     * Check connection aliveness status.
     */
    private void checkConnection() {
        Boolean hasRemoteSrvNodes = null;

        if (server.spi.failureDetectionTimeoutEnabled() && !failureThresholdReached &&
            U.currentTimeMillis() - server.locNode.lastExchangeTime() >= connCheckThreshold &&
            server.spiStateCopy() == CONNECTED &&
            (hasRemoteSrvNodes = server.ring.hasRemoteServerNodes())) {

            if (log.isInfoEnabled())
                log.info("Local node seems to be disconnected from topology (failure detection timeout " +
                    "is reached) [failureDetectionTimeout=" + server.spi.failureDetectionTimeout() +
                    ", connCheckFreq=" + connCheckFreq + ']');

            failureThresholdReached = true;

            // Reset sent time deliberately to force sending connection check message.
            lastTimeConnCheckMsgSent = 0;
        }

        long elapsed = (lastTimeConnCheckMsgSent + connCheckFreq) - U.currentTimeMillis();

        if (elapsed > 0)
            return;

        if (hasRemoteSrvNodes == null)
            hasRemoteSrvNodes = server.ring.hasRemoteServerNodes();

        if (hasRemoteSrvNodes) {
            sendMessageAcrossRing(new TcpDiscoveryConnectionCheckMessage(server.locNode));

            lastTimeConnCheckMsgSent = U.currentTimeMillis();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return String.format("%s, nextNode=[%s]", super.toString(), next);
    }
}

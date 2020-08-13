package org.apache.ignite.internal.util.nio.filter;

import java.io.IOException;
import java.util.Deque;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridSelectorNioSessionImpl;
import org.apache.ignite.internal.util.nio.operation.SessionWriteRequest;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;
import org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage;
import org.apache.ignite.spi.communication.tcp.messages.SystemMessage;

import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.NIO_OPERATION;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.CONN_IDX_META;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.CONSISTENT_ID_META;

public class NioRecoveryFilter extends GridAbstractNioFilter {
    /** */
    private final TcpCommunicationMetricsListener metricsLsnr;

    /** */
    private final int ackSendThreshold;

    /** */
    private final IgniteLogger log;

    /** */
    public NioRecoveryFilter(TcpCommunicationMetricsListener metricsLsnr, int ackSendThreshold, IgniteLogger log){
        this.metricsLsnr = metricsLsnr;
        this.ackSendThreshold = ackSendThreshold;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
        assert ses instanceof GridSelectorNioSessionImpl;

        onSessionOpened((GridSelectorNioSessionImpl)ses);
        proceedSessionOpened(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
        assert ses instanceof GridSelectorNioSessionImpl;

        onSessionClosed((GridSelectorNioSessionImpl)ses);
        proceedSessionClosed(ses);
    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        assert ses instanceof GridSelectorNioSessionImpl;
        assert msg instanceof Message;

        onMessageReceived((GridSelectorNioSessionImpl)ses, (Message) msg);
        proceedMessageReceived(ses, msg);
    }

    /** */
    private void onSessionOpened(GridSelectorNioSessionImpl ses) {
        GridNioRecoveryDescriptor recoveryDesc = ses.outRecoveryDescriptor();
        if (recoveryDesc != null && !recoveryDesc.messagesRequests().isEmpty()) {
            Deque<SessionWriteRequest> futs = recoveryDesc.messagesRequests();

            if (log.isDebugEnabled())
                log.debug("Resend messages [rmtNode=" + recoveryDesc.node().id() + ", msgCnt=" + futs.size() + ']');

            for (SessionWriteRequest fut : futs)
                ses.offerWrite(fut.recover(ses));
        }
    }

    /** */
    private void onSessionClosed(GridSelectorNioSessionImpl ses) {
        // Since ses is in closed state, no write requests will be added.
        ses.removeMeta(NIO_OPERATION.ordinal());

        GridNioRecoveryDescriptor outRecovery = ses.outRecoveryDescriptor();
        GridNioRecoveryDescriptor inRecovery = ses.inRecoveryDescriptor();

        IOException err = new IOException("Failed to send message (connection was closed): " + ses);

        // Poll will update recovery data.
        for (SessionWriteRequest req = ses.pollRequest(); req != null; req = ses.pollRequest()) {
            assert req.system() || outRecovery != null;

            if (req.system())
                req.onError(err);
        }

        if (outRecovery != null)
            outRecovery.release();

        if (inRecovery != null && inRecovery != outRecovery)
            inRecovery.release();
    }

    /** */
    private void onMessageReceived(GridSelectorNioSessionImpl ses, Message msg) {
        ConnectionKey connKey = ses.meta(CONN_IDX_META);

        if (connKey != null && !connKey.dummy()) {
            Object consistentId = ses.meta(CONSISTENT_ID_META);

            assert consistentId != null;

            if (msg instanceof RecoveryLastReceivedMessage) {
                RecoveryLastReceivedMessage msg0 = (RecoveryLastReceivedMessage)msg;
                GridNioRecoveryDescriptor recovery = ses.outRecoveryDescriptor();

                assert recovery != null && msg0.received() > 0;

                metricsLsnr.onMessageReceived(msg0, consistentId);

                if (log.isDebugEnabled()) {
                    log.debug("Received recovery acknowledgement [rmtNode=" + connKey.nodeId() +
                        ", connIdx=" + connKey.connectionIndex() +
                        ", rcvCnt=" + msg0.received() + ']');
                }

                recovery.ackReceived(msg0.received());

                return;
            }

            GridNioRecoveryDescriptor recovery = ses.inRecoveryDescriptor();

            if (recovery != null) {
                assert !(msg instanceof SystemMessage) : msg;

                long rcvCnt = recovery.onReceived();

                if (rcvCnt % ackSendThreshold == 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("Send recovery acknowledgement [rmtNode=" + connKey.nodeId() +
                            ", connIdx=" + connKey.connectionIndex() +
                            ", rcvCnt=" + rcvCnt + ']');
                    }

                    ses.send(new RecoveryLastReceivedMessage(rcvCnt));

                    recovery.lastAcknowledged(rcvCnt);
                }
            }
        }
    }
}

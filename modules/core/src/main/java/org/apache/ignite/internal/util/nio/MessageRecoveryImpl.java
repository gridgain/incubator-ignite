package org.apache.ignite.internal.util.nio;

import java.util.concurrent.ConcurrentMap;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.GridConcurrentFactory;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;

import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.usePairedConnections;

public class MessageRecoveryImpl implements MessageRecovery {
    private final ClusterDiscovery discovery;

    /** Recovery descs. */
    private final ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> recoveryDescs = GridConcurrentFactory.newMap();

    /** Out rec descs. */
    private final ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> outRecDescs = GridConcurrentFactory.newMap();

    /** In rec descs. */
    private final ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> inRecDescs = GridConcurrentFactory.newMap();

    /**
     * @param node Node.
     * @param key Connection key.
     * @return Recovery descriptor for incoming connection.
     */
    @Override public GridNioRecoveryDescriptor inRecoveryDescriptor(ClusterNode node, ConnectionKey key) {
        if (usePairedConnections && usePairedConnections(node, pairedConnectionAttr))
            return recoveryDescriptor(inRecDescs, true, node, key);
        else
            return recoveryDescriptor(recoveryDescs, false, node, key);
    }

    /**
     * @param node Node.
     * @param key Connection key.
     * @return Recovery descriptor for outgoing connection.
     */
    @Override public GridNioRecoveryDescriptor outRecoveryDescriptor(ClusterNode node, ConnectionKey key) {
        if (usePairedConnections && usePairedConnections(node, pairedConnectionAttr))
            return recoveryDescriptor(outRecDescs, true, node, key);
        else
            return recoveryDescriptor(recoveryDescs, false, node, key);
    }

    /**
     * @param recoveryDescs Descriptors map.
     * @param pairedConnections {@code True} if in/out connections pair is used for communication with node.
     * @param node Node.
     * @param key Connection key.
     * @return Recovery receive data for given node.
     */
    private GridNioRecoveryDescriptor recoveryDescriptor(
        ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> recoveryDescs,
        boolean pairedConnections,
        ClusterNode node,
        ConnectionKey key) {
        GridNioRecoveryDescriptor recovery = recoveryDescs.get(key);

        if (recovery == null) {
            if (log.isDebugEnabled())
                log.debug("Missing recovery descriptor for the node (will create a new one) " +
                    "[locNodeId=" + discovery.localNode().id() +
                    ", key=" + key + ", rmtNode=" + node + ']');

            int maxSize = Math.max(cfg.messageQueueLimit(), cfg.ackSendThreshold());

            int queueLimit = cfg.unackedMsgsBufferSize() != 0 ? cfg.unackedMsgsBufferSize() : (maxSize * 128);

            GridNioRecoveryDescriptor old = recoveryDescs.putIfAbsent(key,
                recovery = new GridNioRecoveryDescriptor(pairedConnections, queueLimit, node, log));

            if (old != null) {
                recovery = old;

                if (log.isDebugEnabled())
                    log.debug("Will use existing recovery descriptor: " + recovery);
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Initialized recovery descriptor [desc=" + recovery + ", maxSize=" + maxSize +
                        ", queueLimit=" + queueLimit + ']');
            }
        }

        return recovery;
    }

    /**
     * @return Recovery descs.
     */
    public ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> recoveryDescs() {
        return recoveryDescs;
    }

    /**
     * @return Out rec descs.
     */
    public ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> outRecDescs() {
        return outRecDescs;
    }
}

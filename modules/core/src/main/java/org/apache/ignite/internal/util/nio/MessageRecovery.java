package org.apache.ignite.internal.util.nio;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;

public interface MessageRecovery extends NioService {
    /**
     * @param node Node.
     * @param key Connection key.
     * @return Recovery descriptor for incoming connection.
     */
    GridNioRecoveryDescriptor inRecoveryDescriptor(ClusterNode node, ConnectionKey key);

    /**
     * @param node Node.
     * @param key Connection key.
     * @return Recovery descriptor for outgoing connection.
     */
    GridNioRecoveryDescriptor outRecoveryDescriptor(ClusterNode node, ConnectionKey key);
}

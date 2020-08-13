package org.apache.ignite.spi.communication.tcp.messages;

import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

public class SystemMessageFactoryProvider implements MessageFactoryProvider {
    /** {@inheritDoc} */
    @Override public void registerAll(IgniteMessageFactory factory) {
        factory.register(TcpCommunicationSpi.HANDSHAKE_MSG_TYPE, HandshakeMessage::new);
        factory.register(TcpCommunicationSpi.HANDSHAKE_MSG_TYPE_V2, HandshakeMessage2::new);
        factory.register(TcpCommunicationSpi.NODE_ID_MSG_TYPE, NodeIdMessage::new);
        factory.register(TcpCommunicationSpi.RECOVERY_LAST_ID_MSG_TYPE, RecoveryLastReceivedMessage::new);
        factory.register(TcpCommunicationSpi.HANDSHAKE_WAIT_MSG_TYPE, HandshakeWaitMessage::new);
    }
}

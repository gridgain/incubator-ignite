package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;

/**
 *
 */
class PendingMessage {
    /** */
    TcpDiscoveryAbstractMessage msg;

    /** */
    final boolean customMsg;

    /** */
    final IgniteUuid id;

    /**
     * @param msg Message.
     */
    PendingMessage(TcpDiscoveryAbstractMessage msg) {
        assert msg != null && msg.id() != null : msg;

        this.msg = msg;

        id = msg.id();

        customMsg = msg instanceof TcpDiscoveryCustomEventMessage;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PendingMessage.class, this);
    }
}

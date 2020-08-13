package org.apache.ignite.spi.communication.tcp.internal;

import org.apache.ignite.internal.util.nio.GridCommunicationClient;

public interface CommunicationClientRegistry {
    GridCommunicationClient client(ConnectionKey key);
}

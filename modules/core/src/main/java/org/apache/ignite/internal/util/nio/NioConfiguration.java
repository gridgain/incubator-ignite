package org.apache.ignite.internal.util.nio;

public class NioConfiguration {
    private final boolean usePairedConnections;

    public NioConfiguration(boolean usePairedConnections) {
        this.usePairedConnections = usePairedConnections;
    }

    public boolean usePairedConnections() {
        return usePairedConnections;
    }

    public int messageQueueLimit() {
        return 0;
    }

    public int unackedMsgsBufferSize() {
        return 0;
    }

    public int ackSendThreshold() {
        return 0;
    }
}

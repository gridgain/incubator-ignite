package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

public class TestCustomDiscoveryAckMessage implements DiscoveryCustomMessage {
    final IgniteUuid uuid = IgniteUuid.randomUuid();

    final int corrId;

    public TestCustomDiscoveryAckMessage(int corrId) {
        this.corrId = corrId;
    }

    @Override
    public IgniteUuid id() {
        return uuid;
    }

    @Nullable
    @Override
    public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public boolean stopProcess() {
        return false;
    }

    @Override
    public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer, DiscoCache discoCache) {
        return null;
    }
}

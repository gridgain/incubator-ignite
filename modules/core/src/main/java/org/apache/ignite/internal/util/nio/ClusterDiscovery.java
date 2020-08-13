package org.apache.ignite.internal.util.nio;

import java.util.UUID;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFeatures;

public interface ClusterDiscovery extends NioService {
    ClusterNode localNode();

    default UUID localNodeId() {
        return localNode().id();
    }

    ClusterNode remoteNode(UUID nodeId);

    boolean alive(UUID nodeId);

    boolean known(UUID nodeId);

    boolean allNodesSupport(IgniteFeatures features);
}

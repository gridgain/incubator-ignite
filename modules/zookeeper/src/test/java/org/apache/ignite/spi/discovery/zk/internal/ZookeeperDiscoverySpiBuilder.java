package org.apache.ignite.spi.discovery.zk.internal;

import java.util.HashMap;
import java.util.Map;
import org.apache.curator.test.TestingCluster;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiNodeAuthenticator;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS;

/**
 * Builder for Zookeeper Discovery SPI for test purposes.
 */
public class ZookeeperDiscoverySpiBuilder {
    private static final int DFLT_SESSION_TIMEOUT = 10_000;

    private long joinTimeout;

    private long sessionTimeout;

    private boolean clientReconnectDisabled;

    private IgniteOutClosure<DiscoverySpiNodeAuthenticator> auth;

    private String zkRootPath;

    public ZookeeperDiscoverySpiBuilder setJoinTimeout(long joinTimeout) {
        this.joinTimeout = joinTimeout;

        return this;
    }

    public ZookeeperDiscoverySpiBuilder setSessionTimeout(long sessionTimeout) {
        this.sessionTimeout = sessionTimeout;

        return this;
    }

    public ZookeeperDiscoverySpiBuilder setClientReconnectDisabled(boolean clientReconnectDisabled) {
        this.clientReconnectDisabled = clientReconnectDisabled;

        return this;
    }

    public ZookeeperDiscoverySpiBuilder setAuth(IgniteOutClosure<DiscoverySpiNodeAuthenticator> auth) {
        this.auth = auth;

        return this;
    }

    public ZookeeperDiscoverySpiBuilder setZkRootPath(String zkRootPath) {
        this.zkRootPath = zkRootPath;

        return this;
    }

    public ZookeeperDiscoverySpi build(@NotNull String igniteNodeName, @NotNull TestingCluster zkCluster) {
        ZookeeperDiscoverySpi spi = new ZookeeperDiscoverySpi();

        spi.setJoinTimeout(joinTimeout);
        spi.setSessionTimeout(sessionTimeout > 0 ? sessionTimeout : DFLT_SESSION_TIMEOUT);
        spi.setClientReconnectDisabled(clientReconnectDisabled);

        spi.setZkConnectionString(zkCluster.getConnectString());

        if (zkRootPath != null)
            spi.setZkRootPath(zkRootPath);

        // Set authenticator for basic sanity tests.
        if (auth != null) {
            spi.setAuthenticator(auth.apply());

            spi.setInternalListener(new IgniteDiscoverySpiInternalListener() {
                @Override public void beforeJoin(ClusterNode locNode, IgniteLogger log) {
                    ZookeeperClusterNode locNode0 = (ZookeeperClusterNode)locNode;

                    Map<String, Object> attrs = new HashMap<>(locNode0.getAttributes());

                    attrs.put(ATTR_SECURITY_CREDENTIALS, new SecurityCredentials(null, null, igniteNodeName));

                    locNode0.setAttributes(attrs);
                }

                @Override public boolean beforeSendCustomEvent(DiscoverySpi spi, IgniteLogger log, DiscoverySpiCustomMessage msg) {
                    return false;
                }
            });
        }

        return spi;
    }
}

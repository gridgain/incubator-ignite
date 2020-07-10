/*
 * Copyright (C) GridGain Systems. All Rights Reserved.
 * _________        _____ __________________        _____
 * __  ____/___________(_)______  /__  ____/______ ____(_)_______
 * _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 * / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 * \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.discovery.isolated;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_NODE_CONSISTENT_ID;

/**
 * Special isolated node.
 */
public class IsolatedNode implements IgniteClusterNode {
    /** */
    private final UUID id;

    /** */
    private final IgniteProductVersion ver;

    /** Consistent ID. */
    private Object consistentId;

    /** Node attributes. */
    private Map<String, Object> attrs;

    /** */
    private volatile ClusterMetrics metrics = new ClusterMetricsSnapshot();

    /** */
    private volatile Map<Integer, CacheMetrics> cacheMetrics = Collections.emptyMap();

    /**
     * @param id Node ID.
     * @param attrs Node attributes.
     * @param ver Node version.
     */
    public IsolatedNode(UUID id, Map<String, Object> attrs, IgniteProductVersion ver) {
        this.id = id;
        this.attrs = U.sealMap(attrs);
        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public Object consistentId() {
        return consistentId;
    }

    /** {@inheritDoc} */
    @Override public <T> T attribute(String name) {
        return (T)attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() {
        return metrics;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        return attrs;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> addresses() {
        return Collections.singleton("127.0.0.1");
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        return Collections.singleton("localhost");
    }

    /** {@inheritDoc} */
    @Override public long order() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDaemon() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isClient() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void setConsistentId(Serializable consistentId) {
        this.consistentId = consistentId;

        final Map<String, Object> map = new HashMap<>(attrs);

        map.put(ATTR_NODE_CONSISTENT_ID, consistentId);

        attrs = Collections.unmodifiableMap(map);
    }

    /** {@inheritDoc} */
    @Override public void setMetrics(ClusterMetrics metrics) {
        this.metrics = metrics;
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, CacheMetrics> cacheMetrics() {
        return cacheMetrics;
    }

    /** {@inheritDoc} */
    @Override public void setCacheMetrics(Map<Integer, CacheMetrics> cacheMetrics) {
        this.cacheMetrics = cacheMetrics != null ? cacheMetrics : Collections.emptyMap();
    }
}

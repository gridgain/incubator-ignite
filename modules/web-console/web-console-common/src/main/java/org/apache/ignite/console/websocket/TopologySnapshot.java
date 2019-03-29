/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.websocket;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeBean;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CLUSTER_NAME;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CLIENT_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IPS;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.sortAddresses;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.splitAddresses;

/**
 * Topology snapshot POJO.
 */
public class TopologySnapshot {
    /** Optional Ignite cluster ID. */
    private static final String IGNITE_CLUSTER_ID = "IGNITE_CLUSTER_ID";

    /** */
    private String clusterId;

    /** */
    private String clusterName;

    /** */
    private Collection<UUID> nids;

    /** */
    private Map<UUID, String> addrs;

    /** */
    private Map<UUID, Boolean> clients;

    /** */
    private String clusterVerStr;

    /** */
    private IgniteProductVersion clusterVer;

    /** */
    private boolean active;

    /** */
    private boolean secured;

    /**
     * Helper method to get attribute.
     *
     * @param attrs Map with attributes.
     * @param name Attribute name.
     * @return Attribute value.
     */
    private static <T> T attribute(Map<String, Object> attrs, String name) {
        return (T)attrs.get(name);
    }

    /**
     * Default constructor for serialization.
     */
    public TopologySnapshot() {
        // No-op.
    }

    /**
     * @param nodes Nodes.
     */
    public TopologySnapshot(Collection<GridClientNodeBean> nodes) {
        int sz = nodes.size();

        nids = new ArrayList<>(sz);
        addrs = U.newHashMap(sz);
        clients = U.newHashMap(sz);
        active = false;
        secured = false;

        for (GridClientNodeBean node : nodes) {
            UUID nid = node.getNodeId();

            nids.add(nid);

            Map<String, Object> attrs = node.getAttributes();

            if (F.isEmpty(clusterId))
                clusterId = attribute(attrs, IGNITE_CLUSTER_ID);

            if (F.isEmpty(clusterName))
                clusterName = attribute(attrs, IGNITE_CLUSTER_NAME);

            Boolean client = attribute(attrs, ATTR_CLIENT_MODE);

            clients.put(nid, client);

            Collection<String> nodeAddrs = client
                ? splitAddresses(attribute(attrs, ATTR_IPS))
                : node.getTcpAddresses();

            String firstIP = F.first(sortAddresses(nodeAddrs));

            addrs.put(nid, firstIP);

            String nodeVerStr = attribute(attrs, ATTR_BUILD_VER);

            IgniteProductVersion nodeVer = IgniteProductVersion.fromString(nodeVerStr);

            if (clusterVer == null || clusterVer.compareTo(nodeVer) > 0) {
                clusterVer = nodeVer;
                clusterVerStr = nodeVerStr;
            }
        }

        // TODO WC-1006 How to fix?
        if (F.isEmpty(clusterId)) {
            clusterId = "NO_CLUSTER_ID";
            clusterName = "NO_CLUSTER_NAME";
        }
    }

    /**
     * @return Cluster id.
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * @param clusterId Cluster id.
     */
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    /**
     * @return Cluster name.
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * @param clusterName Cluster name.
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * @return Cluster version.
     */
    public String getClusterVersion() {
        return clusterVerStr;
    }

    /**
     * @param clusterVerStr Cluster version.
     */
    public void setClusterVersion(String clusterVerStr) {
        this.clusterVerStr = clusterVerStr;
    }

    /**
     * @return Cluster active flag.
     */
    public boolean isActive() {
        return active;
    }

    /**
     * @param active New cluster active state.
     */
    public void setActive(boolean active) {
        this.active = active;
    }

    /**
     * @return {@code true} If cluster has configured security.
     */
    public boolean isSecured() {
        return secured;
    }

    /**
     * @param secured Configured security flag.
     */
    public void setSecured(boolean secured) {
        this.secured = secured;
    }

    /**
     * @return Cluster nodes IDs.
     */
    public Collection<UUID> getNids() {
        return nids;
    }

    /**
     * @param nids Cluster nodes IDs.
     */
    public void setNids(Collection<UUID> nids) {
        this.nids = nids;
    }

    /**
     * @return Cluster nodes with IPs.
     */
    public Map<UUID, String> getAddresses() {
        return addrs;
    }

    /**
     * @param addrs Cluster nodes with IPs.
     */
    public void setAddresses(Map<UUID, String> addrs) {
        this.addrs = addrs;
    }

    /**
     * @return Cluster nodes with client mode flag.
     */
    public Map<UUID, Boolean> getClients() {
        return clients;
    }

    /**
     * @param clients Cluster nodes with client mode flag.
     */
    public void setClients(Map<UUID, Boolean> clients) {
        this.clients = clients;
    }

    /**
     * @return Cluster version.
     */
    public IgniteProductVersion clusterVersion() {
        return clusterVer;
    }

    /**
     * @return String with short node UUIDs.
     */
    public String nid8() {
        return nids.stream().map(nid -> U.id8(nid).toUpperCase()).collect(Collectors.joining(",", "[", "]"));
    }

    /**
     * @param prev Previous topology.
     * @return {@code true} in case if current topology is a new cluster.
     */
    public boolean differentCluster(TopologySnapshot prev) {
        return prev == null || F.isEmpty(prev.nids) || Collections.disjoint(nids, prev.nids);
    }

    /**
     * @param prev Previous topology.
     * @return {@code true} in case if current topology is the same cluster, but topology changed.
     */
    public boolean topologyChanged(TopologySnapshot prev) {
        return prev != null && !prev.nids.equals(nids);
    }
}

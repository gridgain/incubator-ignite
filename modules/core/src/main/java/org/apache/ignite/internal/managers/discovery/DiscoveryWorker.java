package org.apache.ignite.internal.managers.discovery;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.failure.RestartProcessFailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.lang.GridTuple6;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.plugin.segmentation.SegmentationPolicy;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.plugin.segmentation.SegmentationPolicy.NOOP;

/** Worker for discovery events. */
class DiscoveryWorker extends GridWorker {
    private GridDiscoveryManager gridDiscoveryManager;
    /** */
    DiscoCache discoCache;

    /** Event queue. */
    private final BlockingQueue<GridTuple6<Integer, AffinityTopologyVersion, ClusterNode,
        DiscoCache, Collection<ClusterNode>, DiscoveryCustomMessage>> evts = new LinkedBlockingQueue<>();

    /** Restart process handler. */
    private final RestartProcessFailureHandler restartProcHnd = new RestartProcessFailureHandler();

    /** Stop node handler. */
    private final StopNodeFailureHandler stopNodeHnd = new StopNodeFailureHandler();

    /** Node segmented event fired flag. */
    private boolean nodeSegFired;

    /** last take timestamp. */
    private volatile long lastTake;

    /**
     *
     */
    DiscoveryWorker(GridDiscoveryManager gridDiscoveryManager) {
        super(gridDiscoveryManager.ctx.igniteInstanceName(), "disco-event-worker", gridDiscoveryManager.log, gridDiscoveryManager.ctx.workersRegistry());
        this.gridDiscoveryManager = gridDiscoveryManager;
    }

    /**
     * Method is called when any discovery event occurs.
     *
     * @param type Discovery event type. See {@link DiscoveryEvent} for more details.
     * @param topVer Topology version.
     * @param node Remote node this event is connected with.
     * @param discoCache Discovery cache.
     * @param topSnapshot Topology snapshot.
     */
    @SuppressWarnings("RedundantTypeArguments")
    private void recordEvent(int type, long topVer, ClusterNode node, DiscoCache discoCache, Collection<ClusterNode> topSnapshot) {
        assert node != null;

        if (gridDiscoveryManager.ctx.event().isRecordable(type)) {
            DiscoveryEvent evt = new DiscoveryEvent();

            evt.node(gridDiscoveryManager.ctx.discovery().localNode());
            evt.eventNode(node);
            evt.type(type);
            evt.topologySnapshot(topVer, U.<ClusterNode, ClusterNode>arrayList(topSnapshot, GridDiscoveryManager.FILTER_NOT_DAEMON));

            if (type == EVT_NODE_METRICS_UPDATED)
                evt.message("Metrics were updated: " + node);

            else if (type == EVT_NODE_JOINED)
                evt.message("Node joined: " + node);

            else if (type == EVT_NODE_LEFT)
                evt.message("Node left: " + node);

            else if (type == EVT_NODE_FAILED)
                evt.message("Node failed: " + node);

            else if (type == EVT_NODE_SEGMENTED)
                evt.message("Node segmented: " + node);

            else if (type == EVT_CLIENT_NODE_DISCONNECTED)
                evt.message("Client node disconnected: " + node);

            else if (type == EVT_CLIENT_NODE_RECONNECTED)
                evt.message("Client node reconnected: " + node);

            else
                assert false;

            gridDiscoveryManager.ctx.event().record(evt, discoCache);
        }
    }

    /**
     * @param type Event type.
     * @param topVer Topology version.
     * @param node Node.
     * @param discoCache Discovery cache.
     * @param topSnapshot Topology snapshot.
     * @param data Custom message.
     */
    void addEvent(
        int type,
        AffinityTopologyVersion topVer,
        ClusterNode node,
        DiscoCache discoCache,
        Collection<ClusterNode> topSnapshot,
        @Nullable DiscoveryCustomMessage data
    ) {
        assert node != null : data;

        log.info("Last take " + lastTake + ", evts.size() " + evts.size());

        evts.add(new GridTuple6<>(type, topVer, node, discoCache, topSnapshot, data));
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException {
        while (!isCancelled()) {
            try {
                body0();
            }
            catch (InterruptedException e) {
                if (!isCancelled)
                    gridDiscoveryManager.ctx.failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, e));

                throw e;
            }
            catch (Throwable t) {
                U.error(log, "Exception in discovery event worker thread.", t);

                FailureType type = t instanceof OutOfMemoryError ? CRITICAL_ERROR : SYSTEM_WORKER_TERMINATION;

                gridDiscoveryManager.ctx.failure().process(new FailureContext(type, t));

                throw t;
            }
        }
    }

    /** @throws InterruptedException If interrupted. */
    @SuppressWarnings("DuplicateCondition")
    private void body0() throws InterruptedException {
        GridTuple6<Integer, AffinityTopologyVersion, ClusterNode, DiscoCache, Collection<ClusterNode>,
            DiscoveryCustomMessage> evt = evts.take();
        
        lastTake = System.currentTimeMillis();
        
        int type = evt.get1();

        AffinityTopologyVersion topVer = evt.get2();

        if (type == EVT_NODE_METRICS_UPDATED && (discoCache == null || topVer.compareTo(discoCache.version()) < 0))
            return;

        ClusterNode node = evt.get3();

        boolean isDaemon = node.isDaemon();

        boolean segmented = false;

        if (evt.get4() != null)
            discoCache = evt.get4();

        switch (type) {
            case EVT_NODE_JOINED: {
                assert !gridDiscoveryManager.discoOrdered || topVer.topologyVersion() == node.order() : "Invalid topology version [topVer=" + topVer +
                    ", node=" + node + ']';

                try {
                    gridDiscoveryManager.checkAttributes(F.asList(node));
                }
                catch (IgniteCheckedException e) {
                    U.warn(log, e.getMessage()); // We a have well-formed attribute warning here.
                }

                if (!isDaemon) {
                    if (!gridDiscoveryManager.isLocDaemon) {
                        if (log.isInfoEnabled())
                            log.info("Added new node to topology: " + node);

                        gridDiscoveryManager.ackTopology(topVer.topologyVersion(), type, node, true);
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Added new node to topology: " + node);
                }
                else if (log.isDebugEnabled())
                    log.debug("Added new daemon node to topology: " + node);

                break;
            }

            case EVT_NODE_LEFT: {
                // Check only if resolvers were configured.
                if (gridDiscoveryManager.hasRslvrs)
                    gridDiscoveryManager.segChkWrk.scheduleSegmentCheck();

                if (!isDaemon) {
                    if (!gridDiscoveryManager.isLocDaemon) {
                        if (log.isInfoEnabled())
                            log.info("Node left topology: " + node);

                        gridDiscoveryManager.ackTopology(topVer.topologyVersion(), type, node, true);
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Node left topology: " + node);
                }
                else if (log.isDebugEnabled())
                    log.debug("Daemon node left topology: " + node);

                break;
            }

            case EVT_CLIENT_NODE_DISCONNECTED: {
                // No-op.

                break;
            }

            case EVT_CLIENT_NODE_RECONNECTED: {
                if (log.isInfoEnabled())
                    log.info("Client node reconnected to topology: " + node);

                if (!gridDiscoveryManager.isLocDaemon)
                    gridDiscoveryManager.ackTopology(topVer.topologyVersion(), type, node, true);

                break;
            }

            case EVT_NODE_FAILED: {
                // Check only if resolvers were configured.
                if (gridDiscoveryManager.hasRslvrs)
                    gridDiscoveryManager.segChkWrk.scheduleSegmentCheck();

                if (!isDaemon) {
                    if (!gridDiscoveryManager.isLocDaemon) {
                        U.warn(log, "Node FAILED: " + node);

                        gridDiscoveryManager.ackTopology(topVer.topologyVersion(), type, node, true);
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Node FAILED: " + node);
                }
                else if (log.isDebugEnabled())
                    log.debug("Daemon node FAILED: " + node);

                break;
            }

            case EVT_NODE_SEGMENTED: {
                assert F.eqNodes(gridDiscoveryManager.localNode(), node);

                if (nodeSegFired) {
                    if (log.isDebugEnabled()) {
                        log.debug("Ignored node segmented event [type=EVT_NODE_SEGMENTED, " +
                            "node=" + node + ']');
                    }

                    return;
                }

                // Ignore all further EVT_NODE_SEGMENTED events
                // until EVT_NODE_RECONNECTED is fired.
                nodeSegFired = true;

                gridDiscoveryManager.lastLoggedTop.set(0);

                segmented = true;

                if (!gridDiscoveryManager.isLocDaemon)
                    U.warn(log, "Local node SEGMENTED: " + node);
                else if (log.isDebugEnabled())
                    log.debug("Local node SEGMENTED: " + node);

                break;
            }

            case EVT_DISCOVERY_CUSTOM_EVT: {
                if (gridDiscoveryManager.ctx.event().isRecordable(EVT_DISCOVERY_CUSTOM_EVT)) {
                    DiscoveryCustomEvent customEvt = new DiscoveryCustomEvent();

                    customEvt.node(gridDiscoveryManager.ctx.discovery().localNode());
                    customEvt.eventNode(node);
                    customEvt.type(type);
                    customEvt.topologySnapshot(topVer.topologyVersion(), evt.get5());
                    customEvt.affinityTopologyVersion(topVer);
                    customEvt.customMessage(evt.get6());

                    if (evt.get4() == null) {
                        assert discoCache != null : evt.get6();

                        evt.set4(discoCache);
                    }

                    gridDiscoveryManager.ctx.event().record(customEvt, evt.get4());
                }

                return;
            }

            // Don't log metric update to avoid flooding the log.
            case EVT_NODE_METRICS_UPDATED:
                break;

            default:
                assert false : "Invalid discovery event: " + type;
        }

        recordEvent(type, topVer.topologyVersion(), node, evt.get4(), evt.get5());

        if (segmented)
            onSegmentation();
    }

    /**
     *
     */
    private void onSegmentation() {
        SegmentationPolicy segPlc = gridDiscoveryManager.ctx.config().getSegmentationPolicy();

        // Always disconnect first.
        try {
            gridDiscoveryManager.getSpi().disconnect();
        }
        catch (IgniteSpiException e) {
            U.error(log, "Failed to disconnect discovery SPI.", e);
        }

        switch (segPlc) {
            case RESTART_JVM:
                gridDiscoveryManager.ctx.failure().process(new FailureContext(FailureType.SEGMENTATION, null), restartProcHnd);

                break;

            case STOP:
                gridDiscoveryManager.ctx.failure().process(new FailureContext(FailureType.SEGMENTATION, null), stopNodeHnd);

                break;

            default:
                assert segPlc == NOOP : "Unsupported segmentation policy value: " + segPlc;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DiscoveryWorker.class, this);
    }
}

package org.apache.ignite.testframework.junits;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.multijvm.IgniteNodeRunner;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import static org.apache.ignite.internal.GridKernalState.DISCONNECTED;
import static org.apache.ignite.testframework.config.GridTestProperties.IGNITE_CFG_PREPROCESSOR_CLS;

public class TestClusterManager {
    /** */
    private static final int DFLT_TOP_WAIT_TIMEOUT = 2000;

    private final IgniteLogger log;

    private final TestConfigurationProvider configProvider;

    private final TestIgniteInstanceNameProvider instanceNameProvider;

    private final boolean startRemoteAsDefault;

    public TestClusterManager(IgniteLogger log,
        TestConfigurationProvider provider,
        TestIgniteInstanceNameProvider testIgniteInstanceNameProvider, boolean startRemoteAsDefault) {
        this.log = log;
        configProvider = provider;
        this.instanceNameProvider = testIgniteInstanceNameProvider;
        this.startRemoteAsDefault = startRemoteAsDefault;
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return {@code True} if the name of the grid indicates that it was the first started (on this JVM).
     */
    protected boolean isFirstGrid(String igniteInstanceName) {
        return instanceNameProvider.instanceNameWasGeneratedByProvider(igniteInstanceName)
            && instanceNameProvider.getTestIgniteInstanceIndex(igniteInstanceName) == 0;
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return <code>True</code> if test was run in multi-JVM mode and grid with this name was started at another JVM.
     */
    protected boolean isRemoteJvm(String igniteInstanceName) {
        IgniteEx ignite = ignite(igniteInstanceName);

        return ignite != null ?
            ignite instanceof IgniteProcessProxy :
            startRemoteAsDefault && !isFirstGrid(igniteInstanceName);
    }

    /**
     * @param idx Grid index.
     * @return <code>True</code> if test was run in multi-JVM mode and grid with this ID was started at another JVM.
     */
    protected boolean isRemoteJvm(int idx) {
        return idx != 0 && isRemoteJvm(instanceNameProvider.getTestIgniteInstanceName(idx));
    }

    /**
     * @return Started grid.
     * @throws Exception If anything failed.
     */
    protected IgniteEx startGrid() throws Exception {
        return startGrid(instanceNameProvider.getTestIgniteInstanceName());
    }

    /**
     * Starts new grid with given index.
     *
     * @param idx Index of the grid to start.
     * @return Started grid.
     * @throws Exception If anything failed.
     */
    protected IgniteEx startGrid(int idx) throws Exception {
        return startGrid(instanceNameProvider.getTestIgniteInstanceName(idx));
    }


    /**
     * Starts new grid with given name.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected IgniteEx startGrid(String igniteInstanceName) throws Exception {
        return startGrid(igniteInstanceName, (GridSpringResourceContext)null);
    }

    /**
     * Starts new grid with given configuration.
     *
     * @param cfg Ignite configuration.
     * @return Started grid.
     * @throws Exception If anything failed.
     */
    protected IgniteEx startGrid(IgniteConfiguration cfg) throws Exception {
        return startGrid(cfg.getIgniteInstanceName(), cfg, null);
    }

    /**
     * Starts new grid with given index and Spring application context.
     *
     * @param idx Index of the grid to start.
     * @param ctx Spring context.
     * @return Started grid.
     * @throws Exception If anything failed.
     */
    protected Ignite startGrid(int idx, GridSpringResourceContext ctx) throws Exception {
        return startGrid(instanceNameProvider.getTestIgniteInstanceName(idx), ctx);
    }

    /**
     * Starts new grid with given name.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param ctx Spring context.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected IgniteEx startGrid(String igniteInstanceName, GridSpringResourceContext ctx) throws Exception {
        return startGrid(igniteInstanceName, configProvider.readyConfiguration(igniteInstanceName), ctx);
    }


    /**
     * Starts new grid at another JVM with given name.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param cfg Ignite configuration.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected IgniteEx startRemoteGrid(
        String igniteInstanceName,
        IgniteConfiguration cfg
    ) throws Exception {
        return startRemoteGrid(igniteInstanceName, cfg, ignite(0), true, null);
    }

    /**
     * Starts new grid at another JVM with given name.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param cfg Ignite configuration.
     * @param locNode Local node.
     * @param resetDiscovery Reset DiscoverySpi.
     * @param additionalRemoteJvmArgs
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected IgniteEx startRemoteGrid(String igniteInstanceName, IgniteConfiguration cfg, IgniteEx locNode,
        boolean resetDiscovery, List<String> additionalRemoteJvmArgs) throws Exception {

        if (locNode != null) {
            DiscoverySpi discoverySpi = locNode.configuration().getDiscoverySpi();

            if (discoverySpi != null && !(discoverySpi instanceof TcpDiscoverySpi)) {
                try {
                    // Clone added to support ZookeeperDiscoverySpi.
                    Method m = discoverySpi.getClass().getDeclaredMethod("cloneSpiConfiguration");

                    m.setAccessible(true);

                    cfg.setDiscoverySpi((DiscoverySpi)m.invoke(discoverySpi));

                    resetDiscovery = false;
                }
                catch (NoSuchMethodException e) {
                    // Ignore.
                }
            }
        }

        return new IgniteProcessProxy(cfg, log, locNode, resetDiscovery, additionalRemoteJvmArgs);
    }


    /**
     * Starts new grid with given name.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param ctx Spring context.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected IgniteEx startGrid(String igniteInstanceName, IgniteConfiguration cfg, GridSpringResourceContext ctx)
        throws Exception {
        if (!isRemoteJvm(igniteInstanceName)) {
            IgniteUtils.setCurrentIgniteName(igniteInstanceName);

            try {
                String cfgProcClsName = System.getProperty(IGNITE_CFG_PREPROCESSOR_CLS);

                if (cfgProcClsName != null) {
                    try {
                        Class<?> cfgProc = Class.forName(cfgProcClsName);

                        Method method = cfgProc.getMethod("preprocessConfiguration", IgniteConfiguration.class);

                        if (!Modifier.isStatic(method.getModifiers()))
                            throw new Exception("Non-static pre-processor method in pre-processor class: " + cfgProcClsName);

                        method.invoke(null, cfg);
                    }
                    catch (Exception e) {
                        log.error("Failed to pre-process IgniteConfiguration using pre-processor class: " + cfgProcClsName);

                        throw new IgniteException(e);
                    }
                }

                IgniteEx node = (IgniteEx)IgnitionEx.start(cfg, ctx);

                IgniteConfiguration nodeCfg = node.configuration();

                log.info("Node started with the following configuration [id=" + node.cluster().localNode().id()
                    + ", marshaller=" + nodeCfg.getMarshaller()
                    + ", discovery=" + nodeCfg.getDiscoverySpi()
                    + ", binaryCfg=" + nodeCfg.getBinaryConfiguration()
                    + ", lateAff=" + nodeCfg.isLateAffinityAssignment() + "]");

                return node;
            }
            finally {
                IgniteUtils.setCurrentIgniteName(null);
            }
        }
        else
            return startRemoteGrid(igniteInstanceName, null);
    }


    /**
     * Start specified amount of nodes.
     *
     * @param cnt Nodes count.
     * @param client Client mode.
     * @param cfgUrl Config URL.
     * @return First started node.
     * @throws Exception If failed.
     */
    protected Ignite startGridsWithSpringCtx(int cnt, boolean client, String cfgUrl) throws Exception {
        assert cnt > 0;

        Ignite ignite = null;

        for (int i = 0; i < cnt; i++) {
            if (ignite == null)
                ignite = startGridWithSpringCtx(instanceNameProvider.getTestIgniteInstanceName(i), client, cfgUrl);
            else
                startGridWithSpringCtx(instanceNameProvider.getTestIgniteInstanceName(i), client, cfgUrl);
        }

        checkTopology(cnt);

        assert ignite != null;

        return ignite;
    }


    /**
     * @param igniteInstanceName Ignite instance name.
     * @param cancel Cancel flag.
     * @param awaitTop Await topology change flag.
     */
    protected void stopGrid(@Nullable String igniteInstanceName, boolean cancel, boolean awaitTop) {
        try {
            IgniteEx ignite = ignite(igniteInstanceName);

            assert ignite != null : "Ignite returned null grid for name: " + igniteInstanceName;

            UUID id = ignite instanceof IgniteProcessProxy ? ignite.localNode().id() : ignite.context().localNodeId();

            log.info(">>> Stopping grid [name=" + ignite.name() + ", id=" + id + ']');

            if (!isRemoteJvm(igniteInstanceName))
                G.stop(igniteInstanceName, cancel);
            else
                IgniteProcessProxy.stop(igniteInstanceName, cancel);

            if (awaitTop)
                awaitTopologyChange();
        }
        catch (IllegalStateException ignored) {
            // Ignore error if grid already stopped.
        }
        catch (Throwable e) {
            log.error("Failed to stop grid [igniteInstanceName=" + igniteInstanceName + ", cancel=" + cancel + ']', e);

//TODO            stopGridErr = true;
        }
    }


    /**
     * @param cnt Grid count
     * @throws Exception If an error occurs.
     */
    @SuppressWarnings({"BusyWait"})
    protected void checkTopology(int cnt) throws Exception {
        for (int j = 0; j < 10; j++) {
            boolean topOk = true;

            for (int i = 0; i < cnt; i++) {
                if (cnt != ignite(i).cluster().nodes().size()) {
                    U.warn(log, "Grid size is incorrect (will re-run check in 1000 ms) " +
                        "[name=" + ignite(i).name() + ", size=" + ignite(i).cluster().nodes().size() + ']');

                    topOk = false;

                    break;
                }
            }

            if (topOk)
                return;
            else
                Thread.sleep(1000);
        }

        throw new Exception("Failed to wait for proper topology [expCnt=" + cnt +
            ", actualTopology=" + ignite(0).cluster().nodes() + ']');
    }


    /**
     * Gets grid for given test.
     *
     * @return Grid for given test.
     */
    protected IgniteEx ignite() {
        return ignite(instanceNameProvider.getTestIgniteInstanceName());
    }

    /**
     * Gets grid for given index.
     *
     * @param idx Index.
     * @return Grid instance.
     */
    protected IgniteEx ignite(int idx) {
        return ignite(instanceNameProvider.getTestIgniteInstanceName(idx));
    }

    /**
     * Gets grid for given name.
     *
     * @param name Name.
     * @return Grid instance.
     */
    protected IgniteEx ignite(String name) {
        if (!isRemoteJvm(name))
            return (IgniteEx)G.ignite(name);
        else {
            if (GridTestUtils.isCurrentJvmRemote())
                return IgniteNodeRunner.startedInstance();
            else
                return IgniteProcessProxy.ignite(name);
        }
    }

    /**
     * Starts new grid with given name.
     *
     * @param gridName Grid name.
     * @param client Client mode.
     * @param cfgUrl Config URL.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected Ignite startGridWithSpringCtx(String gridName, boolean client, String cfgUrl) throws Exception {
        IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgMap =
            IgnitionEx.loadConfigurations(cfgUrl);

        IgniteConfiguration cfg = F.first(cfgMap.get1());

        cfg.setIgniteInstanceName(gridName);
        cfg.setClientMode(client);

        return IgnitionEx.start(cfg, cfgMap.getValue());
    }




    /**
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private void awaitTopologyChange() throws IgniteInterruptedCheckedException {
        for (Ignite g : G.allGrids()) {
            final GridKernalContext ctx = ((IgniteKernal)g).context();

            if (ctx.isStopping() || ctx.gateway().getState() == DISCONNECTED || !g.active())
                continue;

            AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();
            AffinityTopologyVersion exchVer = ctx.cache().context().exchange().readyAffinityVersion();

            if (!topVer.equals(exchVer)) {
                log.info("Topology version mismatch [node=" + g.name() +
                    ", exchVer=" + exchVer +
                    ", topVer=" + topVer + ']');

                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();
                        AffinityTopologyVersion exchVer = ctx.cache().context().exchange().readyAffinityVersion();

                        return exchVer.equals(topVer);
                    }
                }, DFLT_TOP_WAIT_TIMEOUT);
            }
        }
    }

    /**
     * @param expSize Expected nodes number.
     * @throws Exception If failed.
     */
    protected void waitForTopology(final int expSize) throws Exception {
        Assert.assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                List<Ignite> nodes = G.allGrids();

                if (nodes.size() != expSize) {
                    log.info("Wait all nodes [size=" + nodes.size() + ", exp=" + expSize + ']');

                    return false;
                }

                for (Ignite node : nodes) {
                    try {
                        IgniteFuture<?> reconnectFut = node.cluster().clientReconnectFuture();

                        if (reconnectFut != null && !reconnectFut.isDone()) {
                            log.info("Wait for size on node, reconnect is in progress [node=" + node.name() + ']');

                            return false;
                        }

                        int sizeOnNode = node.cluster().nodes().size();

                        if (sizeOnNode != expSize) {
                            log.info("Wait for size on node [node=" + node.name() + ", size=" + sizeOnNode + ", exp=" + expSize + ']');

                            return false;
                        }
                    }
                    catch (IgniteClientDisconnectedException e) {
                        log.info("Wait for size on node, node disconnected [node=" + node.name() + ']');

                        return false;
                    }
                }

                return true;
            }
        }, 30_000));
    }

    public IgniteEx startGrid(Function<IgniteConfiguration, IgniteConfiguration> transformer) throws Exception {
        return startGrid(transformer.apply(configProvider.readyConfiguration(instanceNameProvider.getTestIgniteInstanceName())));
    }

    public IgniteEx startGrid(String igniteInstanceName, Function<IgniteConfiguration, IgniteConfiguration> transformer) throws Exception {
        return startGrid(transformer.apply(configProvider.readyConfiguration(igniteInstanceName)));
    }
}

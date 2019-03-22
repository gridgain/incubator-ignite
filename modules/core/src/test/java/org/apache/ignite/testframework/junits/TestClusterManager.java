package org.apache.ignite.testframework.junits;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.multijvm.IgniteNodeRunner;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

import static org.apache.ignite.testframework.config.GridTestProperties.IGNITE_CFG_PREPROCESSOR_CLS;

public class TestClusterManager {

    private final String igniteName;

    private final IgniteLogger log;

    private final TestConfigurationProvider configProvider;

    public TestClusterManager(String name, IgniteLogger log,
        TestConfigurationProvider provider) {
        igniteName = name;
        this.log = log;
        configProvider = provider;
    }


    /**
     * @return Started grid.
     * @throws Exception If anything failed.
     */
    protected IgniteEx startGrid() throws Exception {
        return startGrid(igniteName);
    }

    /**
     * Starts new grid with given index.
     *
     * @param idx Index of the grid to start.
     * @return Started grid.
     * @throws Exception If anything failed.
     */
    protected IgniteEx startGrid(int idx) throws Exception {
        return startGrid(getTestIgniteInstanceName(idx));
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
        return startGrid(getTestIgniteInstanceName(idx), ctx);
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
                ignite = startGridWithSpringCtx(getTestIgniteInstanceName(i), client, cfgUrl);
            else
                startGridWithSpringCtx(getTestIgniteInstanceName(i), client, cfgUrl);
        }

        checkTopology(cnt);

        assert ignite != null;

        return ignite;
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
        return ignite(igniteName);
    }

    /**
     * Gets grid for given index.
     *
     * @param idx Index.
     * @return Grid instance.
     */
    protected IgniteEx ignite(int idx) {
        return ignite(getTestIgniteInstanceName(idx));
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
     * @param idx Index of the Ignite instance.
     * @return Indexed Ignite instance name.
     */
    public String getTestIgniteInstanceName(int idx) {
        return igniteName + idx;
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



}
